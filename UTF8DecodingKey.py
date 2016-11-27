#!/usr/bin/env python
import boto
import boto.s3
import boto.utils
from boto.utils import compute_md5, compute_hash
from boto.s3.key import Key

class UTF8DecodingKey(boto.s3.key.Key):
    BufferSize = 131072
    
    def __init__(self, key_or_bucket = None, name = None):
        if isinstance(key_or_bucket, boto.s3.key.Key):
            # We find a key
            self.__dict__.update(key_or_bucket.__dict__)
            if name is not None:
                self.name = name

        else: # This is a bucket
            super(UTF8DecodingKey, self).__init__(key_or_bucket, name)



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
