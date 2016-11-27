#!/usr/bin/env python
import io
import time
import os
import os.path
import logging
from interval import Interval
import threading
from LinkedList import LinkedList
from utilityFunc import *
 
class FSRange():
    """A range used to manage buffered downloads from S3."""
    io_wait = 3.0 # 3 seconds
    def __init__(self):
        self.interval = Interval()
        self.ongoing_intervals = {}
        self.event = threading.Event()
        self.lock = threading.RLock()
    def wait(self):
        self.event.wait(self.io_wait)
    def wake(self, again=True):
        with self.lock:
            e = self.event
            if again:
                self.event = threading.Event()
            e.set()









class FSData():
    """The data (content) associated with a file."""
    stores = [ 'mem', 'disk' ]
    unknown_store = "Unknown store"
    def __init__(self, cache, store, path):
        self.cache = cache
        self.store = store
        self.path = path
        self.props = {}
        self.size = 0
        self.etag = None # Something better ???
        if store == 'mem':
            self.content = io.BytesIO()
        elif store == 'disk':
            previous_file = False
            filename = self.cache.get_cache_filename(self.path)
            if os.path.isfile(filename):
                logging.debug("found previous cache file '%s'" % filename)
                # There's a file already there
                self.content = open(filename, mode='rb+')
                self.update_size()
                self.content.close()
                self.set('new', None) # Not sure it is the latest version
                # Now search for an etag file
                etag_filename = self.cache.get_cache_etags_filename(self.path)
                if os.path.isfile(etag_filename):
                    logging.debug("found previous cache etag file '%s'" % etag_filename)
                    with open(etag_filename, mode='r') as etag_file:
                        self.etag = etag_file.read()
                    previous_file = True
            if not previous_file:
                logging.debug("creating new cache file '%s'" % filename)
                with self.cache.disk_lock:
                    create_dirs_for_file(filename)
                    open(filename, mode='w').close() # To create an empty file (and overwrite a previous file)
                logging.debug("created new cache file '%s'" % filename)
            self.content = None # Not open, yet
        else:
            raise FSData.unknown_store
    def get_lock(self, wait_until_cleared_proplist = None):
        return self.cache.get_lock(self.path, wait_until_cleared_proplist)
    def open(self):
        with self.get_lock():
            if not self.has('open'):
                if self.store == 'disk':
                    filename = self.cache.get_cache_filename(self.path)
                    self.content = open(filename, mode='rb+')
            self.inc('open')
    def close(self):
        with self.get_lock():
            self.dec('open')
            if not self.has('open'):
                if self.store == 'disk':
                    self.content.close()
                    self.content = None
    def update_etag(self, new_etag, wait_until_cleared_proplist = None):
        with self.get_lock(wait_until_cleared_proplist):
            if new_etag != self.etag:
                self.etag = new_etag
                if self.store == 'disk':
                    filename = self.cache.get_cache_etags_filename(self.path)
                    with self.cache.disk_lock:
                        create_dirs_for_file(filename)
                        with open(filename, mode='w') as etag_file:
                            create_dirs_for_file(filename)
                        with open(filename, mode='w') as etag_file:
                            etag_file.write(new_etag)
    def get_current_size(self):
        if self.content:
            with self.get_lock():
                self.content.seek(0,2)
                return self.content.tell()
        else:
            return 0 # There's no content...
    def update_size(self, final=False):
        with self.get_lock():
            if final:
                current_size = 0 # The entry is to be deleted
            else:
                current_size = self.get_current_size()
            delta = current_size - self.size
            self.size = current_size
        with self.cache.data_size_lock:
            self.cache.size[self.store] += delta
    def get_content(self, wait_until_cleared_proplist = None):
        with self.get_lock(wait_until_cleared_proplist):
            if self.store == 'disk':
                filename = self.cache.get_cache_filename(self.path)
                return open(filename, mode='rb+')
            else:
                return self.content
    def get_content_as_string(self):
        if self.store == 'mem':
            with self.get_lock():
                return self.content.getvalue()
        elif self.store == 'disk':
            with self.get_lock():
                self.content.seek(0) # Go to the beginning
                return self.content.read()
        else:
            raise FSData.unknown_store
    def has(self, prop):
        with self.get_lock():
            return prop in self.props
    def get(self, prop):
        with self.get_lock():
            try:
                return self.props[prop]
            except KeyError:
                return None
    def set(self, prop, value):
        with self.get_lock():
            self.props[prop] = value
    def inc(self, prop):
        with self.get_lock():
            try:
                self.props[prop] += 1
            except KeyError:
                self.props[prop] = 1
    def dec(self, prop):
        with self.get_lock():
            try:
                if self.props[prop] > 1:
                    self.props[prop] -= 1
                else:
                    del self.props[prop]
            except KeyError:
                pass # Nothing to do
    def delete(self, prop=None, wait_until_cleared_proplist = None):
        with self.get_lock(wait_until_cleared_proplist):
            if prop == None:
                if self.store == 'disk':
                    filename = self.cache.get_cache_filename(self.path)
                    with self.cache.disk_lock:
                        if os.path.isfile(filename):
                            logging.debug("unlink cache file '%s'" % filename)
                            os.unlink(filename)
                            remove_empty_dirs_for_file(filename)
                    etag_filename = self.cache.get_cache_etags_filename(self.path)
                    with self.cache.disk_lock:
                        if os.path.isfile(etag_filename):
                            logging.debug("unlink cache etag file '%s'" % etag_filename)
                            os.unlink(etag_filename)
                            remove_empty_dirs_for_file(etag_filename)
                self.content = None # If not
                self.update_size(True)
                for p in self.props.keys():
                    self.delete(p)
            elif prop in self.props:
                if prop == 'range':
                    logging.debug('there is a range to delete')
                    data_range = self.get(prop)
                else:
                    data_range = None
                del self.props[prop]
                if data_range:
                    logging.debug('wake after range delete')
                    data_range.wake(False) # To make downloading threads go on... and then exit

                if prop == 'change' and 'invoke_after_change' in self.props:
                    logging.debug('FSData.props[change] removed, now executing invoke_after_change lambda for: ' + self.path)
                    self.get('invoke_after_change')(self.path)
                    del self.props['invoke_after_change'] # cLeanup

    def rename(self, new_path):
        with self.get_lock():
            if self.store == 'disk':
                filename = self.cache.get_cache_filename(self.path)
                new_filename = self.cache.get_cache_filename(new_path)
                etag_filename = self.cache.get_cache_etags_filename(self.path)
                new_etag_filename = self.cache.get_cache_etags_filename(new_path)
                with self.cache.disk_lock:
                    create_dirs_for_file(new_filename)
                    os.rename(filename, new_filename)
                with self.cache.disk_lock:
                    remove_empty_dirs_for_file(filename)
                if os.path.isfile(etag_filename):
                    with self.cache.disk_lock:
                        create_dirs_for_file(new_etag_filename)
                        os.rename(etag_filename, new_etag_filename)
                    with self.cache.disk_lock:
                        remove_empty_dirs_for_file(etag_filename)
                if self.content:
                    self.content = open(new_filename, mode='rb+')
            self.path = new_path









class FSCache():
    """ File System Cache """
    def __init__(self, cache_path=None):
        self.cache_path = cache_path
        self.lock = threading.RLock()
        self.disk_lock = threading.RLock() # To safely remove empty disk directories
        self.data_size_lock = threading.RLock()
        self.reset_all()
    def reset_all(self):
         with self.lock:
             self.entries = {}
             self.new_locks = {} # New locks (still) without entry in the cache
             self.unused_locks = {} # Paths with unused locks that will be removed on the next purge if remain unused
             self.lru = LinkedList()
             self.size = {}
             for store in FSData.stores:
                self.size[store] = 0
    def get_memory_usage(self):
        return [ len(self.entries) ] + [ self.size[store] for store in FSData.stores ]
    def get_cache_filename(self, path):
        return self.cache_path + '/files' + path.encode('utf-8') # path begins with '/'
    def get_cache_etags_filename(self, path):
        return self.cache_path + '/etags' + path.encode('utf-8') # path begins with '/'

    def is_deleting(self, path, prop = 'deleting'):
        if not self.has(path, prop):
             return False
        if self.get(path, prop) == 0:
             return False
        return True

    def is_ready(self, path, proplist = None):
        return self.wait_until_cleared(path, proplist = proplist)

    def wait_until_cleared(self, path, proplist = None, max_retries = 10, wait_time = 1):
        default_proplist = ['deleting', 's3_busy']
        if proplist is None:
            proplist = default_proplist

        for prop in proplist:
            if not self.has(path, prop):
                continue
            cleared = False
            for check_count in range(0, max_retries):
                if check_count:
                    logging.debug("wait_until_cleared %s found something for %s. (%i) "%(prop, path, check_count))
                # the cache/key disappeared
                if not self.has(path, prop):
                    logging.debug("wait_until_cleared %s did not find %s anymore."%(prop, path))
                    cleared = True
                    break
                # the cache got a '.dec()' from do_on_s3_now...
                if self.get(path, prop) == 0:
                    logging.debug("wait_until_cleared %s got all dec for %s anymore."%(prop, path))
                    cleared = True
                    break
                time.sleep(wait_time)

            if not cleared:
#                import inspect
#                inspect_stack = inspect.stack()
#                logger.critical("WAIT_UNTIL_CLEARED stack: '%s'"% pp.pformat(inspect_stack))

                logging.error("wait_until_cleared %s could not clear '%s'" % (prop, path))
                raise Exception("Path has not yet been cleared but operation wants to happen on it '%s' '%s'"%(prop, path))
        return True

    def get_lock(self, path, skip_is_ready = False, wait_until_cleared_proplist = None):
        if not skip_is_ready:
            self.is_ready(path, proplist = wait_until_cleared_proplist)

        with self.lock: # Global cache lock, used only for giving file-level locks
            try:
                return self.entries[path]['lock']
            except KeyError:
                try:
                    return self.new_locks[path]
                except KeyError:
                    new_lock = threading.RLock()
                    self.new_locks[path] = new_lock
                    return new_lock
    def add(self, path):
        with self.get_lock(path):
            if not path in self.entries:
                self.entries[path] = {}
                self.entries[path]['lock'] = self.new_locks[path]
                del self.new_locks[path]
                self.lru.append(path)
    def delete(self, path, prop=None):
        with self.get_lock(path):
            if path in self.entries:
                if prop == None:
                    for p in self.entries[path].keys():
                        self.delete(path, p)
                    del self.entries[path]
                    self.lru.delete(path)
                else:
                    if prop in self.entries[path]:
                        if prop == 'data':
                            data = self.entries[path][prop]
                            data.delete() # To clean stuff, e.g. remove cache files
                        elif prop == 'lock':
                            # Preserve lock, let the unused locks check remove it later
                            self.new_locks[path] = self.entries[path][prop]
                        del self.entries[path][prop]
    def rename(self, path, new_path):
        with self.get_lock(path) and self.get_lock(new_path):
            if path in self.entries:
                self.delete(path, 'key') # Cannot be renamed
                self.delete(new_path) # Assume overwrite
                if 'data' in self.entries[path]:
                    data = self.entries[path]['data']
                    with data.get_lock():
                        data.rename(new_path)
                self.entries[new_path] = copy.copy(self.entries[path])
                self.lru.append(new_path)
                self.lru.delete(path)

# 6.59 working except rename...
#                del self.entries[path] # So that the next reset doesn't delete the entry props

                self.inc(path, 'deleting')
                self.inc(new_path, 's3_busy')

    def get(self, path, prop=None):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        try:
            if prop == None:
                return self.entries[path]
            else:
                return self.entries[path][prop]
        except KeyError:
            return None
    def set(self, path, prop, value):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                if prop in self.entries[path]:
                    self.delete(path, prop)
                self.entries[path][prop] = value
                return True
            return False
    def inc(self, path, prop):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                try:
                    self.entries[path][prop] += 1
                except KeyError:
                    self.entries[path][prop] = 1
    def dec(self, path, prop):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                try:
                    if self.entries[path][prop] > 1:
                        self.entries[path][prop] -= 1
                    else:
                        del self.entries[path][prop]
                except KeyError:
                    pass # Nothing to do

    def reset(self, path, with_deleting = True):
        with self.get_lock(path):
            self.delete(path)
            self.add(path)
            if with_deleting:
                self.inc(path, 'deleting')

    def has(self, path, prop=None):
        self.lru.move_to_the_tail(path) # Move to the tail of the LRU cache
        if prop == None:
            return path in self.entries
        else:
            try:
                return prop in self.entries[path]
            except KeyError:
                return False
    def is_empty(self, path): # To improve readability
        if self.has(path) and not self.has(path, 'attr'):
            return True
        else:
            return False
        ###try:
        ###    return len(self.get(path)) <= 1 # Empty or just with 'lock'
        ###except TypeError: # if get returns None
        ###    return False
    def is_not_empty(self, path): # To improve readability
        if self.has(path) and self.has(path, 'attr'):
            return True
        else:
            return False
        ###try:
        ###    return len(self.get(path)) > 1 # More than just 'lock'
        ###except TypeError: # if get returns None
        ###    return False

