#import logging
#
#class SuppressRunOutOfBandFilter(logging.Filter):
#    def filter(self, record):
#        return "Run out-of-band function" not in record.getMessage()
#
#def install_filter():
#    logger = logging.getLogger("distributed.worker")
#    logger.addFilter(SuppressRunOutOfBandFilter())

import logging

class SuppressRunOutOfBandFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "Run out-of-band function" not in msg

def dask_setup(worker):
    """Hook that runs once the worker is ready"""
    logger = logging.getLogger("distributed")
    logger.addFilter(SuppressRunOutOfBandFilter())
