#!/usr/bin/env python

import argparse
import os
import sys
from utilityFunc import * 
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
from fss3 import FUS3FS
def main():

    try:
        default_aws_region = os.environ['AWS_DEFAULT_REGION']
    except KeyError:
        default_aws_region = 'us-east-1'

    description = """
This is cloud-based file system. All files are stored in Amazon S3 and local caching mechanism is introduced to speed up read and write operations.
In addition, Amazon SNS and SQS are integrated into the file system to invalidate local caching of the rest users if files are changed by a user."""

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('s3path', metavar='S3Path',
                        help='the S3 path to mount in s3://BUCKET/PATH format, ' +
                        'PATH can be empty, can contain subfolders and is created on first mount if not found in the BUCKET')
    parser.add_argument('mountpoint', metavar='LocalPath',
                        help='the local mount point')
    parser.add_argument('--region', default=default_aws_region,
                        help='AWS region to use for SNS and SQS (default is %(default)s)')
    parser.add_argument('--topic', metavar='ARN',
                        help='SNS topic ARN')
    parser.add_argument('--new-queue', action='store_true',
                        help='create a new SQS queue that is deleted on unmount to listen to SNS notifications, ' +
                        'overrides --queue, queue name is BUCKET-PATH-ID with alphanumeric characters only')
    parser.add_argument('--new-queue-with-hostname', action='store_true',
                        help='create a new SQS queue with hostname in queuename, ' +
                        'overrides --queue, queue name is BUCKET-PATH-ID with alphanumeric characters only')
    parser.add_argument('--queue', metavar='NAME',
                        help='SQS queue name to listen to SNS notifications, a new queue is created if it doesn\'t exist')
    parser.add_argument('--queue-wait', metavar='N', type=int, default=20,
                        help='SQS queue wait time in seconds (using long polling, 0 to disable, default is %(default)s seconds)')
    parser.add_argument('--queue-polling', metavar='N', type=int, default=0,
                        help='SQS queue polling interval in seconds (default is %(default)s seconds)')
    parser.add_argument('--hostname',
                        help='public hostname to listen to SNS HTTP notifications')
    parser.add_argument('--use-ec2-hostname', action='store_true',
                        help='get public hostname to listen to SNS HTTP notifications ' +
                        'from EC2 instance metadata (overrides --hostname)')
    parser.add_argument('--port', metavar='N',
                        help='TCP port to listen to SNS HTTP notifications')
    parser.add_argument('--cache-entries', metavar='N', type=int, default=100000,
                        help='max number of entries to cache (default is %(default)s entries)')
    parser.add_argument('--cache-mem-size', metavar='N', type=int, default=128,
                        help='max size of the memory cache in MB (default is %(default)s MB)')
    parser.add_argument('--cache-disk-size', metavar='N', type=int, default=1024,
                        help='max size of the disk cache in MB (default is %(default)s MB)')
    parser.add_argument('--cache-path', metavar='PATH', default='',
                        help='local path to use for disk cache (default is /tmp/yas3fs/BUCKET/PATH)')
    parser.add_argument('--recheck-s3', action='store_true',
                        help='Cached ENOENT (error no entry) rechecks S3 for new file/directory')
    parser.add_argument('--cache-on-disk', metavar='N', type=int, default=0,
                        help='use disk (instead of memory) cache for files greater than the given size in bytes ' +
                        '(default is %(default)s bytes)')
    parser.add_argument('--cache-check', metavar='N', type=int, default=5,
                        help='interval between cache size checks in seconds (default is %(default)s seconds)')
    parser.add_argument('--s3-num', metavar='N', type=int, default=32,
                        help='number of parallel S3 calls (0 to disable writeback, default is %(default)s)')
    parser.add_argument('--s3-retries', metavar='N', type=int, default=3,
                        help='number of of times to retry any s3 write operation (default is %(default)s)')
    parser.add_argument('--s3-retries-sleep', metavar='N', type=int, default=1,
                        help='retry sleep in seconds between s3 write operations (default is %(default)s)')
    parser.add_argument('--s3-use-sigv4', action='store_true',
                        help='use AWS signature version 4 for authentication (required for some regions)')
    parser.add_argument('--s3-endpoint',
                        help='endpoint of the s3 bucket, required with --s3-use-sigv4')
    parser.add_argument('--download-num', metavar='N', type=int, default=4,
                        help='number of parallel downloads (default is %(default)s)')
    parser.add_argument('--download-retries-num', metavar='N', type=int, default=60,
                        help='max number of retries when downloading (default is %(default)s)')
    parser.add_argument('--download-retries-sleep', metavar='N', type=int, default=1,
                        help='how long to sleep in seconds between download retries (default is %(default)s seconds)')
    parser.add_argument('--read-retries-num', metavar='N', type=int, default=10,
                        help='max number of retries when read() is invoked (default is %(default)s)')
    parser.add_argument('--read-retries-sleep', metavar='N', type=int, default=1,
                        help='how long to sleep in seconds between read() retries (default is %(default)s seconds)')
    parser.add_argument('--prefetch-num', metavar='N', type=int, default=2,
                        help='number of parallel prefetching downloads (default is %(default)s)')
    parser.add_argument('--st-blksize', metavar='N', type=int, default=None,
                        help='st_blksize to return to getattr() callers in bytes, optional')
    parser.add_argument('--buffer-size', metavar='N', type=int, default=10240,
                        help='download buffer size in KB (0 to disable buffering, default is %(default)s KB)')
    parser.add_argument('--buffer-prefetch', metavar='N', type=int, default=0,
                        help='number of buffers to prefetch (default is %(default)s)')
    parser.add_argument('--no-metadata', action='store_true',
                        help='don\'t write user metadata on S3 to persist file system attr/xattr')
    parser.add_argument('--prefetch', action='store_true',
                        help='download file/directory content as soon as it is discovered ' +
                        '(doesn\'t download file content if download buffers are used)')
    parser.add_argument('--mp-size',metavar='N', type=int, default=100,
                        help='size of parts to use for multipart upload in MB ' +
                        '(default value is %(default)s MB, the minimum allowed by S3 is 5 MB)')
    parser.add_argument('--mp-num', metavar='N', type=int, default=4,
                        help='max number of parallel multipart uploads per file ' +
                        '(0 to disable multipart upload, default is %(default)s)')
    parser.add_argument('--mp-retries', metavar='N', type=int, default=3,
                        help='max number of retries in uploading a part (default is %(default)s)')
    parser.add_argument('--aws-managed-encryption', action='store_true',
                        help='Enable AWS managed encryption (sets header x-amz-server-side-encryption = AES256)')
    parser.add_argument('--id',
                        help='a unique ID identifying this node in a cluster (default is a UUID)')
    parser.add_argument('--mkdir', action='store_true',
                        help='create mountpoint if not found (and create intermediate directories as required)')
    parser.add_argument('--nonempty', action='store_true',
                        help='allows mounts over a non-empty file or directory')
    parser.add_argument('--uid', metavar='N',
                        help='default UID')
    parser.add_argument('--gid', metavar='N',
                        help='default GID')
    parser.add_argument('--umask', metavar='MASK',
                        help='default umask')
    parser.add_argument('--read-only', action='store_true',
                        help='mount read only')
    parser.add_argument('--expiration', metavar='N', type=int, default=30*24*60*60,
                        help='default expiration for signed URL via xattrs (in seconds, default is 30 days)')
    parser.add_argument('--requester-pays', action='store_true',
                        help='requester pays for S3 interactions, the bucket must have Requester Pays enabled')
    parser.add_argument('--no-allow-other', action='store_true',
                        help='Do not allow other users to access mounted directory')
    parser.add_argument('--with-plugin-file', metavar='FILE',
                        help="FUS3FSPlugin file")
    parser.add_argument('--with-plugin-class', metavar='CLASS',
                        help="FUS3FSPlugin class, if this is not set it will take the first child of FUS3FSPlugin from exception handler file")


    parser.add_argument('-l', '--log', metavar='FILE',
                        help='filename for logs')
    parser.add_argument('--log-mb-size', metavar='N', type=int, default=100,
                        help='max size of log file')
    parser.add_argument('--log-backup-count', metavar='N', type=int, default=10,
                        help='number of backups log files')
    parser.add_argument('--log-backup-gzip', action='store_true',
                        help='flag to gzip backup files')
    parser.add_argument('-f', '--foreground', action='store_true',
                        help='run in foreground')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='show debug info')
    #parser.add_argument('-V', '--version', action='version', version='%(prog)s {version}'.format(version=__version__))

    options = parser.parse_args()

    #global pp
    #pp = pprint.PrettyPrinter(indent=1)

    #global logger
    #logger = logging.getLogger('yas3fs')
    #formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    '''
    if options.log: # Rotate log files at 100MB size
        log_size =  options.log_mb_size *1024*1024
        if options.log_backup_gzip:
            logHandler = CompressedRotatingFileHandler(options.log, maxBytes=log_size, backupCount=options.log_backup_count)
        else:
            logHandler = logging.handlers.RotatingFileHandler(options.log, maxBytes=log_size, backupCount=options.log_backup_count)

        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
    if options.foreground or not options.log:
        logHandler = logging.StreamHandler()
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)

    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    '''
    sys.excepthook = custom_sys_excepthook # This is not working for new threads that start afterwards

    logging.debug("options = %s" % options)

    if options.mkdir:
        create_dirs(options.mountpoint)

    mount_options = {
        'mountpoint':options.mountpoint,
        'fsname':'yas3fs',
        'foreground':options.foreground,
        'allow_other':True,
        'auto_cache':True,
        'atime':False,
        'max_read':131072,
        'max_write':131072,
        'max_readahead':131072,
        'direct_io':True
        }
    if options.no_allow_other:
        mount_options["allow_other"] = False
    if options.uid:
        mount_options['uid'] = options.uid
    if options.gid:
        mount_options['gid'] = options.gid
    if options.umask:
        mount_options['umask'] = options.umask
    if options.read_only:
        mount_options['ro'] = True

    if options.nonempty:
        mount_options['nonempty'] = True

    options.darwin = (sys.platform == "darwin")
    if options.darwin:
        mount_options['volname'] = os.path.basename(options.mountpoint)
        mount_options['noappledouble'] = True
        mount_options['daemon_timeout'] = 3600
        # mount_options['auto_xattr'] = True # To use xattr
        # mount_options['local'] = True # local option is quite unstable
    else:
        mount_options['big_writes'] = True # Not working on OSX

    fuse = FUSE(FUS3FS(options), **mount_options)

if __name__ == '__main__':
    main()
