'''
Class to take a file, compress it, chop it in to pieces of a given size, and upload
the pieces to a given location in S3.
'''

import cStringIO, gzip, logging, os, sys, time
from file_splitter.splitter_factory import SplitterFactory
from gzip_pipe import gzip_file

log=logging.getLogger(__name__)
log.debug('__name__ is %s' % __name__)

class S3_puree_error(Exception):
    pass

def time_method(fn):
    from functools import wraps
    @wraps(fn)
    def time_this(*args, **kwargs):
        t1=time.time()
        r=fn(*args, **kwargs)
        t2=time.time()
        log.debug('func %s: time=%s' % (fn.__name__, t2-t1))
        return r
    return time_this

# we need os.fork:
try: f=os.fork
except AttributeError:
    raise S3_puree_error('S3_puree unsupported on this system (needs os.fork)')

class S3_puree(object):
    MAX_NCHUNKS=200
    def __init__(self, chunksize=1<<24, blocksize=1<<13, cleanup=True): # chunksize about 16Mb
        self.chunksize=chunksize # must be something less than 100Mb (1<<20 = 1Mb)
        self.blocksize=blocksize
        self.cleanup=cleanup

    def put_puree(self, fn, bucket, key_base, use_basename=False):
        fn_gz=self._gzip(fn)
        split_fns=self._split(fn_gz)
        keys=self._upload(bucket, split_fns, key_base, use_basename)

        # cleanup
        if self.cleanup:
            log.debug('cleaning up')
            for fn in split_fns:
                try: os.remove(fn)
                except OSError as e: 
                    log.debug('Error removing %s: %e' % (fn, e))

        return keys

    def get_puree(self, fn, bucket, key_base):
        pass
        # download all pieces
        # concat together
        # gunzip
        # cleanup

    @time_method
    def _gzip(self, fn, suffix='.gz'):
        '''
        gzip a file, without destroying the original.  
        This probably won't work for really huge files.
        It's also really f-----g slow
        '''
        log.debug('_gzip: gzipping %s...' % fn)
        out_fn=gzip_file(fn, suffix=suffix)
        log.debug('_gzip: done')
        return out_fn
    
    @time_method
    def _split(self,fn):
        ''' returns names of split files '''
        n_chunks=os.path.getsize(fn)/self.chunksize
        if n_chunks==0: n_chunks=1
        log.debug('splitting %s into %d chunks' % (fn, n_chunks))
        log.debug('(%d=%d/%d)' % (n_chunks, os.path.getsize(fn), self.chunksize))
        if n_chunks > self.MAX_NCHUNKS:
            raise S3_puree_error('too many chunks: %d > %d' % (n_chunks, self.MAX_NCHUNKS))
        return SplitterFactory.splitToDisk(fn, n_chunks)
        

    @time_method
    def _upload(self, bucket, split_fns, key_base, use_basename):
        keys=[]
        for fn in split_fns:
            if use_basename:
                tail=os.path.basename(fn)
            else:
                tail=fn
            key_name='%s/%s' % (key_base, tail)
            key=bucket.new_key(key_name)
            keys.append(key)
            log.debug('key_name is %s' % key_name)

            pid=os.fork()
            if pid==0:
                try:
                    key.set_contents_from_filename(fn)
                    os._exit(0)
                except Exception as e:
                    log.debug('caught %s: %s' % (type(e), e))
                    # do some effective cleanup, I guess, and then:
                    os._exit(1)

        while True:
            try:
                (pid, status)=os.wait()
            except OSError as e:
                break

        return keys

    def puree_old(self, fn, bucket, key_base):
        ''' 
        Read from the file, one chunk at a time, store to stream object
        fn->fp->compressor->stream->s3

        see http://stackoverflow.com/questions/15754610/how-to-gzip-while-uploading-into-s3-using-boto
        '''
        stream=cStringIO.StringIO() # this acts sort of like a buffer
        compressor=gzip.GzipFile(fn, 'w')
        
        def upload_part():
            key=next_key()
            log.debug('upload_part(%s)' % key)
            stream.seek(0)
            key.set_contents_from_stream(stream)
            stream.seek(0)
            stream.truncate()


        def next_key(n=[0]):
            key_name=key_base+'_%d' % n[0]
            n[0]+=1
            return bucket.new_key(key_name)

        with open(fn) as inf:
            while True:
                chunk=inf.read(self.blocksize)
                if chunk:
                    compressor.write(chunk) # feed compressor
                else:
                    compressor.close()
                    upload_part()
                    break

                if stream.tell() > self.chunksize:
                    upload_part()
                    

                
        
