#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

import sys
from google.cloud import logging as cloud_logging


def get_logger(log_name, log_level=logging.INFO):
    """
    Gets logger with Google cloud logging handler.
    
    :param log_name: log name
    :param log_level: log level

    :type log_name: str
    :type log_level: int

    :returns: logger
    :rtype: logging.Logger
    """
    log_format = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging_client = cloud_logging.Client()
    handler = cloud_logging.handlers.CloudLoggingHandler(logging_client, name=log_name)
    handler.setFormatter(logging.Formatter(log_format))
    logger = logging.getLogger(log_name)
    logger.addHandler(handler)
    local_handler = logging.StreamHandler(sys.stdout)
    local_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(local_handler)
    logger.setLevel(log_level)
    return logger
