#!/usr/bin/env python

import logging
import os
import errno
from sys import exit
from fuse import fuse_get_context
import time

def error_and_exit(error, exitCode=1):
    logging.error(error + ", use -h for help.")
    exit(exitCode)

def create_dirs(dirname):
    logging.debug("create_dirs '%s'" % dirname)
    try:
        if not isinstance(dirname,str):
            dirname = dirname.encode('utf-8')

        os.makedirs(dirname)
        logging.debug("create_dirs '%s' done" % dirname)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dirname):
            logging.debug("create_dirs '%s' already there" % dirname)
            pass
        else:
            raise

    except Exception as exc: # Python >2.5
        logging.debug("create_dirs '%s' ERROR %s" % (dirname, exc))
        raise

def remove_empty_dirs(dirname):
    logging.debug("remove_empty_dirs '%s'" % (dirname))

    try:
        if not isinstance(dirname,str):
            dirname = dirname.encode('utf-8')

        os.removedirs(dirname)
        logging.debug("remove_empty_dirs '%s' done" % (dirname))
    except OSError as exc: # Python >2.5
        if exc.errno == errno.ENOTEMPTY:
            logging.debug("remove_empty_dirs '%s' not empty" % (dirname))
            pass
        else:
            raise
    except Exception as e:
        logging.exception(e)
        logging.error("remove_empty_dirs exception: " + dirname)
        raise e


def create_dirs_for_file(filename):
    logging.debug("create_dirs_for_file '%s'" % filename)
    if not isinstance(filename,str):
        filename = filename.encode('utf-8')

    dirname = os.path.dirname(filename)
    create_dirs(dirname)

def remove_empty_dirs_for_file(filename):
    logging.debug("remove_empty_dirs_for_file '%s'" % filename)
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
    logging.exception("Uncaught Exception: " + str(type) + " " + str(value) + " " + str(tb))
