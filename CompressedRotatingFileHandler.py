#!/usr/bin/env python
import logging
import os

class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """ compress old files
    from http://roadtodistributed.blogspot.com/2011/04/compressed-rotatingfilehandler-for.html
    """
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0):
        logging.handlers.RotatingFileHandler.__init__(self, filename, mode, maxBytes, backupCount, encoding, delay)

    def doRollover(self):
        self.stream.close()
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d.gz" % (self.baseFilename, i)
                dfn = "%s.%d.gz" % (self.baseFilename, i + 1)
                if os.path.exists(sfn):
                    #print "%s -> %s" % (sfn, dfn)
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = self.baseFilename + ".1.gz"
            if os.path.exists(dfn):
                os.remove(dfn)
            import gzip
            try:
                f_in = open(self.baseFilename, 'rb')
                f_out = gzip.open(dfn, 'wb')
                f_out.writelines(f_in)
            except:
                if not os.path.exists(dfn):
                    if os.path.exists(self.baseFilename):
                        os.rename(self.baseFilename, dfn)
            finally:
                if "f_out" in dir() and f_out is not None:
                    f_out.close()
                if "f_in" in dir() and f_in is not None:
                    f_in.close()
            if os.path.exists(self.baseFilename):
                os.remove(self.baseFilename)
            #os.rename(self.baseFilename, dfn)
            #print "%s -> %s" % (self.baseFilename, dfn)
        self.mode = 'w'
        self.stream = self._open()
