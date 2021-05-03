# -*- coding: utf-8 -*-
import logging

LOGFMT = "%(asctime)s %(levelno)s /%(filename)s@%(lineno)s/ %(message)s"
logging.basicConfig(level="INFO", format=LOGFMT)
logger = logging.getLogger(__name__)


    # if logLevel is not None:
    #     getattr(logger, logLevel)(f"Format data for {len(D)} objects in {type(D)}")
