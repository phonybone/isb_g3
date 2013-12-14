import unittest, sys, os, boto, logging
from boto.s3.connection import S3Connection

log=logging.getLogger(__name__) # __name__=t.test_puree
d=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(d)

from G3.S3_puree import S3_puree

class TestPuree(unittest.TestCase):
    def setUp(self):
        self.s3=S3Connection()               # using credentials in ~/.boto
        log.debug('got s3: %s' % self.s3)
        
    def test_ping(self):
        print 'ping!'

    def test_gzip(self):
        s3p=S3_puree(chunksize=1<<14)
        fn=os.path.abspath(os.path.join(os.path.dirname(__file__), 'fodder.txt'))
        self.assertTrue(os.path.exists(fn))
        
        fn_gz=fn+'.gz'
        try: os.remove(fn_gz)
        except: pass

        s3p._gzip(fn)
        self.assertTrue(os.path.exists(fn_gz))
        self.assertTrue(os.path.exists(fn))
        os.remove(fn_gz)

    def test_put_puree(self):
#        fn=os.path.join(d, 'S3_puree', 't', 'OverflowTable')
        fn=os.path.join(d, 'S3_puree', 't', 'fodder.txt')
        log.debug('fn is %s' % fn)
        bucket=self.s3.get_bucket('com.phonybone')
        key_name='tests/S3_puree/fodder.txt'
        key=bucket.delete_key(key_name)
        log.debug('deleted key %s' % key)

        s3p=S3_puree(cleanup=False)
        keys=s3p.put_puree(fn, bucket, key_name, use_basename=True)
        log.debug('%d keys' % len(keys))
        
