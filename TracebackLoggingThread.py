#!/usr/bin/env python
import threading
import logging

class TracebackLoggingThread(threading.Thread):
    def run(self):
        try:
            super(TracebackLoggingThread, self).run()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            logging.exception("Uncaught Exception in Thread")
            raise
