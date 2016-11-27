#!/usr/bin/env python

'''from __future__ import with_statement

import os
import sys
import eirrno
import boto
import logging

from fuse import FUSE, FuseOSError, Operations'''

import urllib
import argparse
import errno
import stat
import time
import os
import os.path
import mimetypes
import sys
import json
import urlparse
import threading
import Queue
import socket
import BaseHTTPServer
import urllib2
import itertools
import base64
import logging
import signal
import io
import re
import uuid
import copy
import traceback
import datetime as dt
import gc # For debug only
import pprint # For debug only

from sys import exit
from functools import wraps

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context

import boto
import boto.s3
import boto.sns
import boto.sqs
import boto.utils

from boto.utils import compute_md5, compute_hash
from boto.s3.key import Key

#from YAS3FSPlugin import YAS3FSPlugin

mimetypes.add_type("image/svg+xml", ".svg", True)
mimetypes.add_type("image/svg+xml", ".svgz", True)

class UTF8DecodingKey(boto.s3.key.Key):
    BufferSize = 131072

    def __init__(self, key_or_bucket=None, name=None):
        if isinstance(key_or_bucket, boto.s3.key.Key):
            # this is a key,
            self.__dict__.update(key_or_bucket.__dict__)
            if name is not None:
                self.name = name
        else:
            # this is a bucket
            super(UTF8DecodingKey, self).__init__(key_or_bucket,name)


    def __str__(self):
        if self.name is None:
            return 'None'
        if isinstance(self.name, str):
            return self.name.decode('utf8', 'replace')

        return self.name

    def compute_md5(self, fp, size=None):
        hex_digest, b64_digest, data_size = compute_md5(fp, buf_size=131072, size=size)

        self.size = data_size
        return (hex_digest, b64_digest)


# Main class
# ==========

class FuS3(Operations):

    '''def __init__(self, root):
        self.root = root'''

    def __init__(self, s3path):
        #logger.info("Version: %s" % __version__)
        # Some constants
        ### self.http_listen_path_length = 30
        self.running = True

        self.check_status_interval = 5.0 # Seconds, no need to configure that
        
        self.s3_retries = 1 # default value
        '''self.s3_retries = options.s3_retries # Maximum number of S3 retries (outside of boto)
        logger.info("s3-retries: '%i'" % self.s3_retries)

        self.s3_retries_sleep = options.s3_retries_sleep # retry sleep in seconds
        logger.info("s3-retries-sleep: '%i' seconds" % self.s3_retries_sleep)

        self.yas3fs_xattrs = [ 'user.yas3fs.bucket', 'user.yas3fs.key', 'user.yas3fs.URL', 'user.yas3fs.signedURL', 'user.yas3fs.expiration' ]'''

        self.multipart_uploads_in_progress = 0

        # Initialization
        #global debug
        #debug = options.debug

        # Parameters and options handling
        self.aws_region = 'us-east-1' #options.region
        s3url = urlparse.urlparse(s3path.lower())
        if s3url.scheme != 's3':
            error_and_exit("The S3 path to mount must be in URL format: s3://BUCKET/PATH")
        self.s3_bucket_name = s3url.netloc
        #logger.info("S3 bucket: '%s'" % self.s3_bucket_name)
        self.s3_prefix = s3url.path.strip('/')
        #logger.info("S3 prefix (can be empty): '%s'" % self.s3_prefix)
        if self.s3_bucket_name == '':
            error_and_exit("The S3 bucket cannot be empty")
        '''self.sns_topic_arn = options.topic
        if self.sns_topic_arn:
            logger.info("AWS region for SNS and SQS: '" + self.aws_region + "'")
            logger.info("SNS topic ARN: '%s'" % self.sns_topic_arn)
        self.sqs_queue_name = options.queue # must be different for each client
        self.new_queue = options.new_queue
        self.new_queue_with_hostname = options.new_queue_with_hostname
        if self.new_queue_with_hostname:
            self.new_queue = self.new_queue_with_hostname

        self.queue_wait_time = options.queue_wait
        self.queue_polling_interval = options.queue_polling
        if self.sqs_queue_name:
            logger.info("SQS queue name: '%s'" % self.sqs_queue_name)
        if self.sqs_queue_name or self.new_queue:
            logger.info("SQS queue wait time (in seconds): '%i'" % self.queue_wait_time)
            logger.info("SQS queue polling interval (in seconds): '%i'" % self.queue_polling_interval)
        self.cache_entries = options.cache_entries
        logger.info("Cache entries: '%i'" % self.cache_entries)
        self.cache_mem_size = options.cache_mem_size * (1024 * 1024) # To convert MB to bytes
        logger.info("Cache memory size (in bytes): '%i'" % self.cache_mem_size)
        self.cache_disk_size = options.cache_disk_size * (1024 * 1024) # To convert MB to bytes
        logger.info("Cache disk size (in bytes): '%i'" % self.cache_disk_size)
        self.cache_on_disk = options.cache_on_disk # Bytes
        logger.info("Cache on disk if file size greater than (in bytes): '%i'" % self.cache_on_disk)
        self.cache_check_interval = options.cache_check # seconds
        logger.info("Cache check interval (in seconds): '%i'" % self.cache_check_interval)
        self.recheck_s3 = options.recheck_s3
        logger.info("Cache ENOENT rechecks S3: %s" % self.recheck_s3)
        self.aws_managed_encryption = options.aws_managed_encryption
        logger.info("AWS Managed Encryption enabled: %s" % self.aws_managed_encryption)

        self.aws_managed_encryption = options.aws_managed_encryption
        logger.info("AWS Managed Encryption enabled: %s" % self.aws_managed_encryption)

        self.st_blksize = None
        if options.st_blksize:
            self.st_blksize = options.st_blksize
            #logger.info("getattr() st_blksize: '%i'" % self.st_blksize)

        if options.use_ec2_hostname:
            instance_metadata = boto.utils.get_instance_metadata() # Very slow (to fail) outside of EC2
            self.hostname = instance_metadata['public-hostname']
        else:
            self.hostname = options.hostname
        if self.hostname:
            logger.info("Public hostname to listen to SNS HTTP notifications: '%s'" % self.hostname)
        self.sns_http_port = int(options.port or '0')
        if options.port:
            import M2Crypto # Required to check integrity of SNS HTTP notifications
            logger.info("TCP port to listen to SNS HTTP notifications: '%i'" % self.sns_http_port)
        self.s3_num = options.s3_num
        logger.info("Number of parallel S3 threads (0 to disable writeback): '%i'" % self.s3_num)
        self.download_num = options.download_num
        logger.info("Number of parallel downloading threads: '%i'" % self.download_num)


        # for https://github.com/danilop/yas3fs/issues/46
        self.download_retries_num = options.download_retries_num
        logger.info("Number download retry attempts: '%i'" % self.download_retries_num)
        self.download_retries_sleep = options.download_retries_sleep
        logger.info("Download retry sleep time seconds: '%i'" % self.download_retries_sleep)

        self.read_retries_num = options.read_retries_num
        logger.info("Number read retry attempts: '%i'" % self.read_retries_num)
        self.read_retries_sleep = options.read_retries_sleep
        logger.info("Read retry sleep time seconds: '%i'" % self.read_retries_sleep)


        self.prefetch_num = options.prefetch_num
        logger.info("Number of parallel prefetching threads: '%i'" % self.prefetch_num)
        self.buffer_size = options.buffer_size * 1024 # To convert KB to bytes
        logger.info("Download buffer size (in KB, 0 to disable buffering): '%i'" % self.buffer_size)
        self.buffer_prefetch = options.buffer_prefetch
        logger.info("Number of buffers to prefetch: '%i'" % self.buffer_prefetch)
        self.write_metadata = not options.no_metadata
        logger.info("Write metadata (file system attr/xattr) on S3: '%s'" % str(self.write_metadata))
        self.full_prefetch = options.prefetch
        logger.info("Download prefetch: '%s'" % str(self.full_prefetch))
        self.multipart_size = options.mp_size * (1024 * 1024) # To convert MB to bytes
        logger.info("Multipart size: '%s'" % str(self.multipart_size))
        self.multipart_num = options.mp_num
        logger.info("Multipart maximum number of parallel threads: '%s'" % str(self.multipart_num))
        self.multipart_retries = options.mp_retries
        logger.info("Multipart maximum number of retries per part: '%s'" % str(self.multipart_retries))
        self.default_expiration = options.expiration
        logger.info("Default expiration for signed URLs via xattrs: '%s'" % str(self.default_expiration))
        self.requester_pays = options.requester_pays
        logger.info("S3 Request Payer: '%s'" % str(self.requester_pays))'''
        self.write_metadata = True #used in set metadata
        self.s3_num = 0 # used in do_on_s3
        self.recheck_s3 = False # used in getattr
        self.st_blksize = None # used in getattr
        self.full_prefetch = False # used in getattr 
        self.aws_managed_encryption = 'store_true'
        self.default_headers = {}
        '''if self.requester_pays:
            self.default_headers = { 'x-amz-request-payer' : 'requester' }'''

        crypto_headers = {}
        if self.aws_managed_encryption:
            crypto_headers = { 'x-amz-server-side-encryption' : 'AES256' }

        self.default_write_headers = copy.copy(self.default_headers)
        self.default_write_headers.update(crypto_headers)

        #self.darwin = options.darwin # To tailor ENOATTR for OS X

        # Internal Initialization
        '''if options.cache_path:
            cache_path = options.cache_path
        else:
            cache_path = '/tmp/yas3fs/' + self.s3_bucket_name
            if not self.s3_prefix == '':
                cache_path += '/' + self.s3_prefix
        logger.info("Cache path (on disk): '%s'" % cache_path)
        self.cache = FSCache(cache_path)
        self.publish_queue = Queue.Queue()
        self.s3_queue = {} # Of Queue.Queue()
        for i in range(self.s3_num):
            self.s3_queue[i] = Queue.Queue()
        self.download_queue = Queue.Queue()
        self.prefetch_queue = Queue.Queue()'''

        # AWS Initialization
        if not self.aws_region in (r.name for r in boto.s3.regions()):
            error_and_exit("wrong AWS region '%s' for S3" % self.aws_region)
        try:
            '''if options.s3_use_sigv4:
                os.environ['S3_USE_SIGV4'] = 'True'
                self.s3 = boto.connect_s3(host=options.s3_endpoint)'''
            if False:
              pass
            else:
                self.s3 = boto.connect_s3()
        except boto.exception.NoAuthHandlerFound:
            error_and_exit("no AWS credentials found")
        if not self.s3:
            error_and_exit("no S3 connection")
        try:
            self.s3_bucket = self.s3.get_bucket(self.s3_bucket_name, headers=self.default_headers, validate=False)
            self.s3_bucket.key_class = UTF8DecodingKey
        except boto.exception.S3ResponseError, e:
            error_and_exit("S3 bucket not found:" + str(e))

        '''pattern = re.compile('[\W_]+') # Alphanumeric characters only, to be used for pattern.sub('', s)

        unique_id_list = []
        if options.id:
            unique_id_list.append(pattern.sub('', options.id))
        unique_id_list.append(str(uuid.uuid4()))
        self.unique_id = '-'.join(unique_id_list)
        logger.info("Unique node ID: '%s'" % self.unique_id)

        if self.sns_topic_arn:
            if not self.aws_region in (r.name for r in boto.sns.regions()):
                error_and_exit("wrong AWS region '%s' for SNS" % self.aws_region)
            self.sns = boto.sns.connect_to_region(self.aws_region)
            if not self.sns:
                error_and_exit("no SNS connection")
            try:
                topic_attributes = self.sns.get_topic_attributes(self.sns_topic_arn)
            except boto.exception.BotoServerError:
                error_and_exit("SNS topic ARN not found in region '%s' " % self.aws_region)
            if not self.sqs_queue_name and not self.new_queue:
                if not (self.hostname and self.sns_http_port):
                    error_and_exit("With and SNS topic either the SQS queue name or the hostname and port to listen to SNS HTTP notifications must be provided")

        if self.sqs_queue_name or self.new_queue:
            self.queue = None

            if not self.sns_topic_arn:
                error_and_exit("The SNS topic must be provided when an SQS queue is used")
            if not self.aws_region in (r.name for r in boto.sqs.regions()):
                error_and_exit("wrong AWS region '" + self.aws_region + "' for SQS")
            self.sqs = boto.sqs.connect_to_region(self.aws_region)
            if not self.sqs:
                error_and_exit("no SQS connection")
            if self.new_queue:
                hostname_array = []
                hostname = ''
                if self.new_queue_with_hostname:
                    import socket
                    hostname = socket.gethostname()
                    # trims to the left side only
                    hostname = re.sub(r'[^A-Za-z0-9\-].*', '', hostname)
                    # removes dashes and other chars
                    hostname = re.sub(r'[^A-Za-z0-9]', '', hostname)
                    hostname_array = [hostname]

                self.sqs_queue_name = '-'.join([ 'yas3fs',
                                               pattern.sub('', self.s3_bucket_name),
                                               pattern.sub('', self.s3_prefix),
                                               hostname,
                                               self.unique_id])
                self.sqs_queue_name = self.sqs_queue_name[:80]  # fix for https://github.com/danilop/yas3fs/issues/40
                self.sqs_queue_name = re.sub(r'-+', '-', self.sqs_queue_name)
                logger.info("Attempting to create SQS queue: " + self.sqs_queue_name)

            else:
                self.queue =  self.sqs.lookup(self.sqs_queue_name)
            if not self.queue:
                try:
                    self.queue = self.sqs.create_queue(self.sqs_queue_name)
                except boto.exception.SQSError, sqsErr:
                    error_and_exit("Unexpected error creating SQS queue:" + str(sqsErr))
            logger.info("SQS queue name (new): '%s'" % self.sqs_queue_name)
            self.queue.set_message_class(boto.sqs.message.RawMessage) # There is a bug with the default Message class in boto

        if self.hostname or self.sns_http_port:
            if not self.sns_topic_arn:
                error_and_exit("The SNS topic must be provided when the hostname/port to listen to SNS HTTP notifications is given")

        if self.sns_http_port:
            if not self.hostname:
                error_and_exit("The hostname must be provided with the port to listen to SNS HTTP notifications")
            ### self.http_listen_path = '/sns/' + base64.urlsafe_b64encode(os.urandom(self.http_listen_path_length))
            self.http_listen_path = '/sns'
            self.http_listen_url = "http://%s:%i%s" % (self.hostname, self.sns_http_port, self.http_listen_path)

        if self.multipart_size < 5242880:
            error_and_exit("The minimum size for multipart upload supported by S3 is 5MB")
        if self.multipart_retries < 1:
            error_and_exit("The number of retries for multipart uploads cannot be less than 1")


        self.plugin = None
        if (options.with_plugin_file):
            self.plugin = YAS3FSPlugin.load_from_file(self, options.with_plugin_file, options.with_plugin_class)
        elif (options.with_plugin_class):
            self.plugin = YAS3FSPlugin.load_from_class(self, options.with_plugin_class)

        if self.plugin:
            self.plugin.logger = logger

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGHUP, self.signal_handler)'''
        self.plugin = None # mimic code above


    # Helpers
    # =======

    '''def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path'''

    # Filesystem methods
    # ==================

    '''def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)'''

    '''def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)'''

    def chmod(self, path, mode):
        print "chmod called"
        '''logger.debug("chmod '%s' '%i'" % (path, mode))

        if self.cache.is_deleting(path):
            logger.debug("chmod path '%s' is deleting -- throwing ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)

        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("chmod '%s' '%i' ENOENT" % (path, mode))
                raise FuseOSError(errno.ENOENT)'''
        try:
            attr = self.get_metadata(path, 'attr')
            if attr < 0:
                return attr
            if attr['st_mode'] != mode:
                attr['st_mode'] = mode
                self.set_metadata(path, 'attr')
            return 0
        except Exception, e:
          print str(e)

    '''def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)'''

    def chown(self, path, uid, gid):
        print "chown called"
        '''logger.debug("chown '%s' '%i' '%i'" % (path, uid, gid))

        if self.cache.is_deleting(path):
            logger.debug("chown path '%s' is deleting -- throwing ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)

        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("chown '%s' '%i' '%i' ENOENT" % (path, uid, gid))
                raise FuseOSError(errno.ENOENT)'''
        try:
            attr = self.get_metadata(path, 'attr')
            if attr < 0:
                return attr
            changed = False
            if uid != -1 and attr['st_uid'] != uid:
                attr['st_uid'] = uid
                changed = True
            if gid != -1 and attr['st_gid'] != gid:
                attr['st_gid'] = gid
                changed = True
            if changed:
                self.set_metadata(path, 'attr')
            return 0
        except Exception, e:
          print str(e)

    '''def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))'''

    def getattr(self, path, fh=None):
            print "getattr called"
            '''logger.debug("getattr -> '%s' '%s'" % (path, fh))
            if self.cache.is_deleting(path):
                logger.debug("getattr path '%s' is deleting -- throwing ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)

            with self.cache.get_lock(path): # To avoid consistency issues, e.g. with a concurrent purge'''
            
            cache = True
            recheck_s3 = False
            '''if self.cache.is_empty(path):
                    logger.debug("getattr <- '%s' '%s' cache ENOENT" % (path, fh))
                    if self.recheck_s3:
                        cache = False
                        recheck_s3 = True
                        logger.debug("getattr rechecking on s3 <- '%s' '%s' cache ENOENT" % (path, fh))
                    else:
                        raise FuseOSError(errno.ENOENT)'''

            attr = self.get_metadata(path, 'attr')
            if attr == None:
                    #print "attr is none"
                    #logger.debug("getattr <- '%s' '%s' ENOENT" % (path, fh))
                raise FuseOSError(errno.ENOENT)
            if attr['st_size'] == 0 and stat.S_ISDIR(attr['st_mode']):
                attr['st_size'] = 4096 # For compatibility...
            attr['st_nlink'] = 1 # Something better TODO ???
                #print attr
            '''if self.st_blksize:
                    attr['st_blksize'] = self.st_blksize

                if self.full_prefetch: # Prefetch
                    if stat.S_ISDIR(attr['st_mode']):
                        self.readdir(path)
                    else:
                        self.check_data(path)
                logger.debug("getattr <- '%s' '%s' '%s'" % (path, fh, attr))'''
            return attr


    '''def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r'''

    def readdir(self, path, fh=None):
        print "readdir called"
        '''logger.debug("readdir '%s' '%s'" % (path, fh))

        if self.cache.is_deleting(path):
            logger.debug("readdir path '%s' is deleting -- throwing ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)

        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("readdir '%s' '%s' ENOENT" % (path, fh))
                raise FuseOSError(errno.ENOENT)
            self.cache.add(path)
            dirs = self.cache.get(path, 'readdir')

            if not dirs:
                logger.debug("readdir '%s' '%s' no cache" % (path, fh))'''
        try:
                full_path = self.join_prefix(path)
                if full_path == '.':
                    full_path = ''
                elif full_path != '' and full_path[-1] != '/':
                    full_path += '/'
                #logger.debug("readdir '%s' '%s' S3 list '%s'" % (path, fh, full_path))
                # encoding for https://github.com/danilop/yas3fs/issues/56
                key_list = self.s3_bucket.list(full_path.encode('utf-8'), '/', headers = self.default_headers, encoding_type='url')
                dirs = ['.', '..']
                for k in key_list:

                    # 'unquoting' for https://github.com/danilop/yas3fs/issues/56
                    k.name = urllib.unquote_plus(str(k.name)).decode('utf-8')

                    #logger.debug("readdir '%s' '%s' S3 list key '%s'" % (path, fh, k))
                    d = k.name[len(full_path):]
                    if len(d) > 0:
                        if d == '.':
                            continue # I need this for whole S3 buckets mounted without a prefix, I use '.' for '/' metadata
                        d_path = k.name[len(self.s3_prefix):]
                        if d[-1] == '/':
                            d = d[:-1]
                        '''if self.cache.is_deleting(d_path):
                            continue'''
                        dirs.append(d)

                # for https://github.com/danilop/yas3fs/issues/56
                convertedDirs = []
                for dir in dirs:
                    convertedDirs.append(unicode(dir))
                dirs = convertedDirs

                #self.cache.set(path, 'readdir', dirs)

                #logger.debug("readdir '%s' '%s' '%s'" % (path, fh, dirs))
                return dirs 

        except Exception, e:
          print str(e)

    '''def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname'''

    '''def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)'''

    def mknod(self, path, mode, dev=None):
        print "mknod called"
        '''logger.debug("mknod '%s' '%i' '%s'" % (path, mode, dev))
        with self.cache.get_lock(path):
            if self.cache.is_not_empty(file):
                logger.debug("mknod '%s' '%i' '%s' cache EEXIST" % (path, mode, dev))
                raise FuseOSError(errno.EEXIST)'''
        try:
            k = self.get_key(path)
            if k:
                logger.debug("mknod '%s' '%i' '%s' key EEXIST" % (path, mode, dev))
                raise FuseOSError(errno.EEXIST)
            #self.cache.add(path)
            now = get_current_time()
            uid, gid = get_uid_gid()
            attr = {}
            attr['st_uid'] = uid
            attr['st_gid'] = gid
            attr['st_mode'] = int(stat.S_IFREG | mode)
            attr['st_atime'] = now
            attr['st_mtime'] = now
            attr['st_ctime'] = now
            attr['st_size'] = 0 # New file
            '''if self.cache_on_disk > 0:
                data = FSData(self.cache, 'mem', path) # New files (almost) always cache in mem - is it ok ???
            else:
                data = FSData(self.cache, 'disk', path)
            self.cache.set(path, 'data', data)
            data.set('change', True)'''
            self.set_metadata(path, 'attr', attr)
            self.set_metadata(path, 'xattr', {})
            #self.add_to_parent_readdir(path)
            #self.publish(['mknod', path])
            return 0
        except Exception, e:
          print str(e)

    '''def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)'''

    def rmdir(self, path):
        print "rmdir called"
        '''logger.debug("rmdir '%s'" % (path))

        if self.cache.is_deleting(path):
            logger.debug("rmdir path '%s' is deleting -- throwing ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)

        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("rmdir '%s' cache ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)'''
        try:
            k = self.get_key(path)
            if not k and not self.cache.has(path) and not self.folder_has_contents(path):
                #logger.debug("rmdir '%s' S3 ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)
            '''dirs = self.cache.get(path, 'readdir')
            if dirs == None:
                if self.folder_has_contents(path, 2): # There is something inside the folder
                    logger.debug("rmdir '%s' S3 ENOTEMPTY" % (path))
                    raise FuseOSError(errno.ENOTEMPTY)
            else:
                if len(dirs) > 2:
                    logger.debug("rmdir '%s' cache ENOTEMPTY" % (path))
                    raise FuseOSError(errno.ENOTEMPTY)
            ###k.delete()
            ###self.publish(['rmdir', path])
            self.cache.reset(path, with_deleting = bool(k)) # Cache invaliation
            self.remove_from_parent_readdir(path)'''
            if k:
                #logger.debug("rmdir '%s' '%s' S3" % (path, k))
                pub = [ 'rmdir', path ]
                cmds = [ [ 'delete', [] , { 'headers': self.default_headers } ] ]
                self.do_on_s3(k, pub, cmds)

            return 0

        except Exception, e:
          print str(e)

    '''def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)'''

    def mkdir(self, path, mode):
        print "mkdir called"
        '''logger.debug("mkdir '%s' '%s'" % (path, mode))
        with self.cache.get_lock(path):
            if self.cache.is_not_empty(path):
                logger.debug("mkdir cache '%s' EEXIST" % self.cache.get(path))
                raise FuseOSError(errno.EEXIST)'''
        
        
        k = self.get_key(path)
        if k and path != '/':
                #logger.debug("mkdir key '%s' EEXIST" % self.cache.get(path))
            raise FuseOSError(errno.EEXIST)
        now = get_current_time()
        uid, gid = get_uid_gid()
        attr = { 'st_uid': uid,
                 'st_gid': gid,
                 'st_atime': now,
                 'st_mtime': now,
                 'st_ctime': now,
                 'st_size': 0,
                 'st_mode': (stat.S_IFDIR | mode)
                }
        '''self.cache.delete(path)
            self.cache.add(path)
            data = FSData(self.cache, 'mem', path)
            self.cache.set(path, 'data', data)
            data.set('change', True)'''
        k = UTF8DecodingKey(self.s3_bucket)
        self.set_metadata(path, 'attr', attr, k)
        self.set_metadata(path, 'xattr', {}, k)
        '''self.cache.set(path, 'key', k)'''
        if path != '/':
            full_path = path + '/'
            #self.cache.set(path, 'readdir', ['.', '..']) # the directory is empty
            #self.add_to_parent_readdir(path)
        else:
            full_path = path # To manage '/' with an empty s3_prefix'''

        if path != '/' or self.write_metadata:
            k.key = self.join_prefix(full_path)
                #logger.debug("mkdir '%s' '%s' '%s' S3" % (path, mode, k))
                ###k.set_contents_from_string('', headers={'Content-Type': 'application/x-directory'})
            pub = [ 'mkdir', path ]
            headers = { 'Content-Type': 'application/x-directory'}
            headers.update(self.default_write_headers)
            cmds = [ [ 'set_contents_from_string', [ '' ], { 'headers': headers } ] ]
            self.do_on_s3(k, pub, cmds)
            #data.delete('change')
            ###if path != '/': ### Do I need this???
            ###    self.publish(['mkdir', path])

        return 0


    '''def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))'''

    def statfs(self, path):
        print "statfs called"
        #logger.debug("statfs '%s'" % (path))
        """Returns a dictionary with keys identical to the statvfs C
           structure of statvfs(3).
           The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
           On Mac OS X f_bsize and f_frsize must be a power of 2
           (minimum 512)."""
        return {
            "f_namemax" : 512,
            "f_bsize" : 1024 * 1024,
            "f_blocks" : 1024 * 1024 * 1024,
            "f_bfree" : 1024 * 1024 * 1024,
            "f_bavail" : 1024 * 1024 * 1024,
            "f_files" : 1024 * 1024 * 1024,
            "f_favail" : 1024 * 1024 * 1024,
            "f_ffree" : 1024 * 1024 * 1024
            }

    '''def unlink(self, path):
        return os.unlink(self._full_path(path))'''

    def unlink(self, path):
        print "unlink called"
        '''logger.debug("unlink '%s'" % (path))

        if self.cache.is_deleting(path):
            logger.debug("unlink path '%s' is deleting -- throwing ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)

        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("unlink '%s' ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)'''
        try:
            k = self.get_key(path)
            if not k: #and not self.cache.has(path):
                #logger.debug("unlink '%s' ENOENT" % (path))
                raise FuseOSError(errno.ENOENT)
            '''self.cache.reset(path, with_deleting = bool(k)) # Cache invaliation
            self.remove_from_parent_readdir(path)'''
            if k:
                #logger.debug("unlink '%s' '%s' S3" % (path, k))
                ###k.delete()
                ###self.publish(['unlink', path])
                pub = [ 'unlink', path ]
                cmds = [ [ 'delete', [], { 'headers': self.default_headers } ] ]
                self.do_on_s3(k, pub, cmds)
                # self.do_on_s3_now(k, pub, cmds)
        except Exception, e:
          print str(e)

        return 0

    '''def symlink(self, name, target):
        return os.symlink(name, self._full_path(target))'''

    def symlink(self, path, link):
        print "symlink called"
        '''logger.debug("symlink '%s' '%s'" % (path, link))
        with self.cache.get_lock(path):
            if self.cache.is_not_empty(path):
                logger.debug("symlink cache '%s' '%s' EEXIST" % (path, link))
                raise FuseOSError(errno.EEXIST)'''
        try:
            k = self.get_key(path)
            if k:
                #logger.debug("symlink key '%s' '%s' EEXIST" % (path, link))
                raise FuseOSError(errno.EEXIST)
            now = get_current_time()
            uid, gid = get_uid_gid()
            attr = {}
            attr['st_uid'] = uid
            attr['st_gid'] = gid
            attr['st_atime'] = now
            attr['st_mtime'] = now
            attr['st_ctime'] = now
            attr['st_size'] = 0
            attr['st_mode'] = (stat.S_IFLNK | 0755)
            '''self.cache.delete(path)
            self.cache.add(path)
            if self.cache_on_disk > 0:
                data = FSData(self.cache, 'mem', path) # New files (almost) always cache in mem - is it ok ???
            else:
                data = FSData(self.cache, 'disk', path)
            self.cache.set(path, 'data', data)
            data.set('change', True)'''
            k = UTF8DecodingKey(self.s3_bucket)
            self.set_metadata(path, 'attr', attr, k)
            self.set_metadata(path, 'xattr', {}, k)
            '''data.open()
            self.write(path, link, 0)
            data.close()'''
            k.key = self.join_prefix(path)
            '''self.cache.set(path, 'key', k)
            self.add_to_parent_readdir(path)
            logger.debug("symlink '%s' '%s' '%s' S3" % (path, link, k))'''
            ###k.set_contents_from_string(link, headers={'Content-Type': 'application/x-symlink'})
            pub = [ 'symlink', path ]
            headers = { 'Content-Type': 'application/x-symlink' }
            headers.update(self.default_write_headers)
            cmds = [ [ 'set_contents_from_string', [ link ], { 'headers': headers } ] ]
            self.do_on_s3(k, pub, cmds)
            #data.delete('change')
            ###self.publish(['symlink', path])

            return 0
        except Exception, e:
          print str(e)

    '''def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))'''

    ### Should work for files in cache but not flushed to S3...
    def rename(self, path, new_path):
        print "rename called"
        '''logger.debug("rename '%s' '%s'" % (path, new_path))

        if self.cache.is_deleting(path):
            logger.debug("rename path '%s' is deleting -- throwing ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)

        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("rename '%s' '%s' ENOENT no '%s' from cache" % (path, new_path, path))
                raise FuseOSError(errno.ENOENT)'''
        try:
            key = self.get_key(path)
            if not key: #and not self.cache.has(path):
                #logger.debug("rename '%s' '%s' ENOENT no '%s'" % (path, new_path, path))
                raise FuseOSError(errno.ENOENT)
            new_parent_path = os.path.dirname(new_path)
            new_parent_key = self.get_key(new_parent_path)
            if not new_parent_key and not self.folder_has_contents(new_parent_path):
                #logger.debug("rename '%s' '%s' ENOENT no parent path '%s'" % (path, new_path, new_parent_path))
                raise FuseOSError(errno.ENOENT)
        except Exception, e:
          print str(e)

        attr = self.getattr(path)
        if stat.S_ISDIR(attr['st_mode']):
            self.rename_path(path, new_path)
        else:
            self.rename_item(path, new_path)
        self.remove_from_parent_readdir(path)
        selfi.add_to_parent_readdir(new_path)

    def rename_path(self, path, new_path):
        #logger.debug("rename_path '%s' -> '%s'" % (path, new_path))
        dirs = self.readdir(path)
        for d in dirs:
            if d in ['.', '..']:
                continue
            d_path = ''.join([path, '/', d])
            d_new_path = ''.join([new_path, '/', d])
            attr = self.getattr(d_path)
            if stat.S_ISDIR(attr['st_mode']):
                self.rename_path(d_path, d_new_path)
            else:
                self.rename_item(d_path, d_new_path)
        self.rename_item(path, new_path, dir=True)

    def rename_item(self, path, new_path, dir=False):
        #logger.debug("rename_item '%s' -> '%s' dir?%s" % (path, new_path, dir))
        source_path = path
        target_path = new_path
        key = self.get_key(source_path)
        #self.cache.rename(source_path, target_path)
        if key: # For files in cache or dir not on S3 but still not flushed to S3
            self.rename_on_s3(key, source_path, target_path, dir)

    def rename_on_s3(self, key, source_path, target_path, dir):
        #logger.debug("rename_on_s3 '%s' '%s' -> '%s' dir?%s" % (key, source_path, target_path, dir))
        # Otherwise we loose the Content-Type with S3 Copy
        key.metadata['Content-Type'] = key.content_type
        ### key.copy(key.bucket.name, target, key.metadata, preserve_acl=False)
        target = self.join_prefix(target_path)
        if dir:
            target += '/'
        pub = [ 'rename', source_path, target_path ]

        if isinstance(target,str):
            target_for_cmd = target.decode('utf-8')
        else:
            target_for_cmd = target

        cmds = [ [ 'copy', [ key.bucket.name, target_for_cmd, key.metadata ],
                   { 'preserve_acl': False , 'encrypt_key':self.aws_managed_encryption } ],
                 [ 'delete', [], { 'headers': self.default_headers } ] ]
        self.do_on_s3(key, pub, cmds)

    '''def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))'''

    '''def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)'''

    def utimens(self, path, times=None):
        print "utimens called"
        '''logger.debug("utimens '%s' '%s'" % (path, times))

        if self.cache.is_deleting(path):
            logger.debug("utimens path '%s' is deleting -- throwing ENOENT" % (path))
            raise FuseOSError(errno.ENOENT)

        with self.cache.get_lock(path):
            if self.cache.is_empty(path):
                logger.debug("utimens '%s' '%s' ENOENT" % (path, times))
                raise FuseOSError(errno.ENOENT)'''
        try:
            now = get_current_time()
            atime, mtime = times if times else (now, now)
            attr = self.get_metadata(path, 'attr')
            if attr < 0:
                return attr
            attr['st_atime'] = atime
            attr['st_mtime'] = mtime
            self.set_metadata(path, 'attr')
            return 0
        except Exception, e:
          print str(e)

    # File methods
    # ============

    def open(self, path, flags):
        print "open called"
        return 0
        #full_path = self._full_path(path)
        #return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        print "create called"
        return 0
        #full_path = self._full_path(path)
        #return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        print "read called" + path
        k = self.get_key(path)
        
        data = k.get_contents_as_string(headers = self.default_headers)
        return data


    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        print "fsync called"
        return self.flush(path, fh)

    # End of system calls
    # ===================

    def get_metadata(self, path, metadata_name, key=None):
        print "get_metadata called"
        '''logger.debug("get_metadata -> '%s' '%s' '%s'" % (path, metadata_name, key))
        with self.cache.get_lock(path): # To avoid consistency issues, e.g. with a concurrent purge
            metadata_values = None
            if self.cache.has(path, metadata_name):
                metadata_values = self.cache.get(path, metadata_name)'''
        try:
            metadata_values = None #mimic code above
            if metadata_values == None:
                metadata_values = {}
                if not key:
                    key = self.get_key(path)
                if not key:
                    #return None
                    if path == '/': # First time mount of a new file system
                       print "path = '/', what to do?"
                       ''' self.mkdir(path, 0755)
                        logger.debug("get_metadata -> '%s' '%s' First time mount"
                                     % (path, metadata_name))
                        return self.cache.get(path, metadata_name)'''
                    else:
                        if not self.folder_has_contents(path):
                            '''self.cache.add(path) # It is empty to cache further checks
                            logger.debug("get_metadata '%s' '%s' no S3 return None"
                                         % (path, metadata_name))'''
                            return None
                else:
                    #print "key found on s3"
                    pass
                    '''logger.debug("get_metadata '%s' '%s' '%s' S3 found"
                                         % (path, metadata_name, key))'''

                if key:
                    s = key.get_metadata(metadata_name)
                else:
                    s = None
                if s:
                    try:
                        metadata_values = json.loads(s)
                    except ValueError: # For legacy attribute encoding
                        for kv in s.split(';'):
                            k, v = kv.split('=')
                            if v.isdigit():
                                metadata_values[k] = int(v)
                            elif v.replace(".", "", 1).isdigit():
                                metadata_values[k] = float(v)
                            else:
                                metadata_values[k] = v
                if metadata_name == 'attr': # Custom exception(s)
                    if key:
                        metadata_values['st_size'] = key.size
                    else:
                        metadata_values['st_size'] = 0
                    if not s: # Set default attr to browse any S3 bucket TODO directories
                        uid, gid = get_uid_gid()
                        metadata_values['st_uid'] = uid
                        metadata_values['st_gid'] = gid
                        if key == None:
                            ### # no key, default to dir
                            metadata_values['st_mode'] = (stat.S_IFDIR | 0755)
                        elif key and key.name != '' and key.name[-1] != '/':
                            metadata_values['st_mode'] = (stat.S_IFREG | 0755)
                        else:
                            metadata_values['st_mode'] = (stat.S_IFDIR | 0755)
                        if key and key.last_modified:
                            now = time.mktime(time.strptime(key.last_modified, "%a, %d %b %Y %H:%M:%S %Z"))
                        else:
                            now = get_current_time()
                        metadata_values['st_mtime'] = now
                        metadata_values['st_atime'] = now
                        metadata_values['st_ctime'] = now
                #self.cache.add(path)
                #self.cache.set(path, metadata_name, metadata_values)i
            #logger.debug("get_metadata <- '%s' '%s' '%s' '%s'" % (path, metadata_name, key, metadata_values))
            return metadata_values
        except Exception, e:
          print str(e)

    def set_metadata(self, path, metadata_name=None, metadata_values=None, key=None):
            #logger.debug("set_metadata '%s' '%s' '%s'" % (path, metadata_name, key))
            #with self.cache.get_lock(path):
            '''if not metadata_values == None:
                self.cache.set(path, metadata_name, metadata_values)
            data = self.cache.get(path, 'data')'''
            data = None
            if self.write_metadata and (key or (not data) or (data and not data.has('change'))):
                # No change in progress, I should write now
                if not key:
                    key = self.get_key(path)
                    #logger.debug("set_metadata '%s' '%s' '%s' Key" % (path, metadata_name, key))
                new_key = False
                if not key and self.folder_has_contents(path):
                    if path != '/' or self.write_metadata:
                        full_path = path + '/'
                        key = UTF8DecodingKey(self.s3_bucket)
                        key.key = self.join_prefix(full_path)
                        new_key = True
                if key:
                    if metadata_name:
                        values = metadata_values
                        if values == None:
                            values = self.cache.get(path, metadata_name)
                        if values == None or not any(values):
                            try:
                                del key.metadata[metadata_name]
                            except KeyError:
                                pass
                        else:
                            try:
                                key.metadata[metadata_name] = json.dumps(values)
                            except UnicodeDecodeError:
                                print "set_metadata '%s' '%s' '%s' cannot decode unicode, not written on S3" % (path, metadata_name, key)
                                '''logger.info("set_metadata '%s' '%s' '%s' cannot decode unicode, not written on S3"
                                            % (path, metadata_name, key))'''
                                pass # Ignore the binary values - something better TODO ???
                    if (not data) or (data and (not data.has('change'))):
                        #logger.debug("set_metadata '%s' '%s' S3" % (path, key))
                        pub = [ 'md', metadata_name, path ]
                        if new_key:
                            #logger.debug("set_metadata '%s' '%s' S3 new key" % (path, key))
                            ### key.set_contents_from_string('', headers={'Content-Type': 'application/x-directory'})
                            headers = { 'Content-Type': 'application/x-directory' }
                            headers.update(self.default_write_headers)

                            cmds = [ [ 'set_contents_from_string', [ '' ], { 'headers': headers } ] ]
                            self.do_on_s3(key, pub, cmds)
                        else:
                            ### key.copy(key.bucket.name, key.name, key.metadata, preserve_acl=False)
                            if isinstance(key.name,str):
                                 key_name = key.name.decode('utf-8')
                            else:
                                 key_name = key.name

                            cmds = [ [ 'copy', [ key.bucket.name, key_name, key.metadata ],
                                       { 'preserve_acl': False, 'encrypt_key':self.aws_managed_encryption } ] ]
                            self.do_on_s3(key, pub, cmds)
                        ###self.publish(['md', metadata_name, path])

            # handle a request to set metadata, but we can't right now because the node is currently
            # in the middle of a 'change' https://github.com/danilop/yas3fs/issues/52
            '''elif self.write_metadata and data and data.has('change'):
                if metadata_name == 'attr' and metadata_values == None:
                    logger.debug("set_metadata: 'change' already in progress, setting FSData.props[invoke_after_change] lambda for self.set_metadata("+path+",attr)")
                    data.set('invoke_after_change',(lambda path: self.set_metadata(path,'attr')))'''

    def remove_prefix(self, keyname):
        if self.s3_prefix == '':
            return '/' + keyname
        return keyname[len(self.s3_prefix):]

    def join_prefix(self, path):
        if self.s3_prefix == '':
            if path != '/':
                return path[1:] # Remove beginning '/'
            else:
                return '.' # To handle '/' with empty s3_prefix
        else:
            return self.s3_prefix + path

    def has_elements(self, iter, num=1):
        #logger.debug("has_element '%s' %i" % (iter, num))
        c = 0
        for k in iter:
            #logger.debug("has_element '%s' -> '%s'" % (iter, k))
            path = k.name[len(self.s3_prefix):]
            #if not self.cache.is_deleting(path):
            c += 1
            if c >= num:
                #logger.debug("has_element '%s' OK" % (iter))
                return True
        #logger.debug("has_element '%s' KO" % (iter))
        return False

    def folder_has_contents(self, path, num=1):
        #logger.debug("folder_has_contents '%s' %i" % (path, num))
        full_path = self.join_prefix(path + '/')
        # encoding for https://github.com/danilop/yas3fs/issues/56
        key_list = self.s3_bucket.list(full_path.encode('utf-8'), '/', headers = self.default_headers)
        return self.has_elements(key_list, num)

    def get_key(self, path, cache=True):
        print "get_key called, path="+path
        '''if self.cache.is_deleting(path):
            logger.debug("get_key path '%s' is deleting -- returning None" % (path))
            return None

        if cache and self.cache.is_ready(path):
            key = self.cache.get(path, 'key')
            if key:
                logger.debug("get_key from cache '%s'" % (path))
                return key
        logger.debug("get_key %s", path)'''
        look_on_S3 = True

        '''refresh_readdir_cache_if_found = False
        if path != '/':
            (parent_path, file) = os.path.split(path)
            dirs = self.cache.get(parent_path, 'readdir')
            if dirs and file not in dirs:
                refresh_readdir_cache_if_found = True
                if not self.recheck_s3:
                    look_on_S3 = False'''
        if look_on_S3:
            #logger.debug("get_key from S3 #1 '%s'" % (path))
            # encoding for https://github.com/danilop/yas3fs/issues/56
            key = self.s3_bucket.get_key(self.join_prefix(path).encode('utf-8'), headers=self.default_headers)
            if not key and path != '/':
                full_path = path + '/'
                #logger.debug("get_key from S3 #2 '%s' '%s'" % (path, full_path))
                # encoding for https://github.com/danilop/yas3fs/issues/56
                key = self.s3_bucket.get_key(self.join_prefix(full_path).encode('utf-8'), headers=self.default_headers)
            if key:
                key = UTF8DecodingKey(key)
                key.name = key.name.decode('utf-8')
                #logger.debug("get_key to cache '%s'" % (path))
                ###self.cache.delete(path) ### ???
                ###self.cache.add(path)
                #self.cache.set(path, 'key', key)

                '''if refresh_readdir_cache_if_found:
                    self.add_to_parent_readdir(path)'''
        else:
            print "get_key not on S3 '%s'" % (path)
            #logger.debug("get_key not on S3 '%s'" % (path))
        if not key:
            print "get_key no '%s'" % (path) 
            #logger.debug("get_key no '%s'" % (path))
        return key

    # faking the funk, get a better wrapper model later
    def withplugin(fn):
        def fn_wrapper(*arg, **karg):
            self = arg[0]
            if self.plugin == None:
                return fn(*arg, **karg)

            try:
                handlerFn = getattr(self.plugin, fn.__name__)
                return handlerFn(fn).__call__(*arg, **karg)
            except:
                return fn(*arg, **karg)
        return fn_wrapper

    @withplugin
    def do_on_s3(self, key, pub, cmds):
        if self.s3_num == 0:
            return self.do_on_s3_now(key, pub, cmds)
        try:
          i = hash(key.name) % self.s3_num # To distribute files consistently across threads
          self.s3_queue[i].put((key, pub, cmds))
        except Exception, e:
          print str(e)

    @withplugin
    def do_on_s3_now(self, key, pub, cmds):
        for c in cmds:
            action = c[0]
            args = None
            kargs = None

            if len(c) > 1:
                args = c[1]
            if len(c) > 2:
                kargs = c[2]

            pub = self.do_cmd_on_s3_now_w_retries(key, pub, action, args, kargs, self.s3_retries)
            '''if pub:
                self.publish(pub)'''

    @withplugin
    def do_cmd_on_s3_now_w_retries(self, key, pub, action, args, kargs, retries = 1):
        last_exception = None
        for tries in range(1, retries +1):
            if tries > 1:
                time.sleep(self.s3_retries_sleep) # Better wait N seconds before retrying
            try:
                #logger.debug("do_cmd_on_s3_now_w_retries try %s action '%s' key '%s' args '%s' kargs '%s'" % (tries, action, key, args, kargs))
                return self.do_cmd_on_s3_now(key, pub, action, args, kargs)
            except Exception, e:
                last_exception = e

        print "do_cmd_on_s3_now_w_retries FAILED '%s' key '%s' args '%s' kargs '%s'" % (action, key, args, kargs)
        #logger.error("do_cmd_on_s3_now_w_retries FAILED '%s' key '%s' args '%s' kargs '%s'" % (action, key, args, kargs))
        print str(last_exception)
        raise last_exception                                            

    @withplugin
    def do_cmd_on_s3_now(self, key, pub, action, args, kargs):
        #logger.debug("do_cmd_on_s3_now action '%s' key '%s' args '%s' kargs '%s'" % (action, key, args, kargs))

        # fuse/yas3fs is version unaware and all operation should
        # happen to the current version
        # also we don't track updated key.version_id in self.cache
        # so it is likely that what was stored has been staled
        key.version_id = None

        try:
            if action == 'delete':
                path = pub[1]
                key.delete()
                #del self.cache.entries[path]

            elif action == 'copy':
                # Otherwise we loose the Content-Type with S3 Copy
                key.metadata['Content-Type'] = key.content_type
                key.copy(*args, **kargs)

                '''path = self.remove_prefix(args[1])

                if path.endswith('/'):
                    # this is a directory, but interally stored w/o
                    # trailing slash
                    path = path[:-1]

                # renaming?
                if path != key.name:
                    # del self.cache.entries[path]
                    if self.cache.has(path, 's3_busy'):
                        self.cache.entries[path]['s3_busy'] = 0'''

            elif action == 'set_contents_from_string':
                key.set_contents_from_string(*args,**kargs)
            elif action == 'set_contents_from_file':
                data = args[0] # First argument must be data

                #if data.cache.is_deleting(data.path):
                    #return None

                try:
                    # ignore deleting flag, though will fail w/ IOError
                    key.set_contents_from_file(data.get_content(wait_until_cleared_proplist = ['s3_busy']),**kargs)
                except IOError as e:
                    #logger.error("set_contents_from_file IOError on " + str(data))
                    raise e
                    #print str(e)

                '''etag = key.etag[1:-1]

                # ignore deleting flag
                with data.get_lock(wait_until_cleared_proplist = ['s3_busy']):
                    data.update_etag(etag, wait_until_cleared_proplist = ['s3_busy'])
                    data.delete('change', wait_until_cleared_proplist = ['s3_busy'])
                pub.append(etag)'''
            elif action == 'multipart_upload':
                pass
                '''data = args[1] # Second argument must be data

                if data.cache.is_deleting(data.path):
                    return None

                full_size = args[2] # Third argument must be full_size
                complete = self.multipart_upload(*args)

                uploaded_key = self.s3_bucket.get_key(key.name.encode('utf-8'), headers=self.default_headers)
                print "Multipart-upload Key Sizes '%s' local: %i remote: %i" %(key, full_size, uploaded_key.size)
                logger.debug("Multipart-upload Key Sizes '%s' local: %i remote: %i" %(key, full_size, uploaded_key.size))
                if full_size != uploaded_key.size:
                     print "Multipart-upload Key Sizes do not match for '%s' local: %i remote: %i" %(key, full_size, uploaded_key.size)
                     logger.error("Multipart-upload Key Sizes do not match for '%s' local: %i remote: %i" %(key, full_size, uploaded_key.size))
                     raise Exception("Multipart-upload KEY SIZES DO NOT MATCH")

                etag = complete.etag[1:-1]
                self.cache.delete(data.path, 'key')

                # ignore deleting flag
                with data.get_lock(wait_until_cleared_proplist = ['s3_busy']):
                    data.update_etag(etag, wait_until_cleared_proplist = ['s3_busy'])
                    data.delete('change', wait_until_cleared_proplist = ['s3_busy'])
                pub.append(etag)'''
            else:
                print "do_cmd_on_s3_now Unknown action '%s'" % action
                #logger.error("do_cmd_on_s3_now Unknown action '%s'" % action)
                # SHOULD THROW EXCEPTION...

        except Exception, e:
          print str(e)


# Utility functions
# =================

def error_and_exit(error, exitCode=1):
    print error + ", use -h for help."
    #logger.error(error + ", use -h for help.")
    exit(exitCode)

def create_dirs(dirname):
    logger.debug("create_dirs '%s'" % dirname)
    try:
        if not isinstance(dirname,str):
            dirname = dirname.encode('utf-8')

        os.makedirs(dirname)
        logger.debug("create_dirs '%s' done" % dirname)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dirname):
            logger.debug("create_dirs '%s' already there" % dirname)
            pass
        else:
            raise

    except Exception as exc: # Python >2.5
        logger.debug("create_dirs '%s' ERROR %s" % (dirname, exc))
        raise

def remove_empty_dirs(dirname):
    logger.debug("remove_empty_dirs '%s'" % (dirname))

    try:
        if not isinstance(dirname,str):
            dirname = dirname.encode('utf-8')

        os.removedirs(dirname)
        logger.debug("remove_empty_dirs '%s' done" % (dirname))
    except OSError as exc: # Python >2.5
        if exc.errno == errno.ENOTEMPTY:
            logger.debug("remove_empty_dirs '%s' not empty" % (dirname))
            pass
        else:
            raise
    except Exception as e:
        logger.exception(e)
        logger.error("remove_empty_dirs exception: " + dirname)
        raise e


def create_dirs_for_file(filename):
    logger.debug("create_dirs_for_file '%s'" % filename)
    if not isinstance(filename,str):
        filename = filename.encode('utf-8')

    dirname = os.path.dirname(filename)
    create_dirs(dirname)

def remove_empty_dirs_for_file(filename):
    logger.debug("remove_empty_dirs_for_file '%s'" % filename)
    if not isinstance(filename,str):
        filename = filename.encode('utf-8')

    dirname = os.path.dirname(filename)
    remove_empty_dirs(dirname)

def get_current_time():
    return time.mktime(time.gmtime())

def get_uid_gid():
    uid, gid, pid = fuse_get_context()
    return int(uid), int(gid)

def thread_is_not_alive(t):
    return t == None or not t.is_alive()

def custom_sys_excepthook(type, value, tb):
    logger.exception("Uncaught Exception: " + str(type) + " " + str(value) + " " + str(tb))

# Start program
# =============

def main(mountpoint, s3path):
    FUSE(FuS3(s3path), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
