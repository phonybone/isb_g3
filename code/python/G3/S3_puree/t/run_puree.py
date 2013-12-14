import logging, os, sys
import logging.config
d=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
print 'd is %s' % d
sys.path.append(os.path.join(d, 'python'))
from G3.S3_puree import S3_puree
from boto.s3.connection import S3Connection

logging.config.fileConfig(os.path.join(d, 'loggers.conf'))
log=logging.getLogger('G3') # __name__=t.test_puree

fn=os.path.join(d, 'python', 'G3', 'S3_puree', 't', 'fodder.txt')
log.debug('fn is %s' % fn)
s3=S3Connection()
bucket=s3.get_bucket('com.phonybone')
key_name='tests/S3_puree/fodder.txt'
key=bucket.delete_key(key_name)
log.debug('deleted key %s' % key)

s3p=S3_puree(chunksize=1<<14)
keys=s3p.put_puree(fn, bucket, key_name, use_basename=True)
log.debug('%d keys' % len(keys))
