#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3
from botocore.exceptions import ProfileNotFound


def get_aws_session(logger, params=None):
    """
    Gets an AWS session.

    :param logger: logger
    :param params: Session parameters

    :type logger: logging.Logger
    :type params: dict

    :returns: AWS session
    :rtype: boto3.session.Session
    """
    try:
        if params is None:
            logger.debug("Getting AWS session using IAM role.")
            aws_session = boto3.Session()
        else:
            logger.debug("Getting AWS session with parameters.")
            aws_session = boto3.Session(**params)

        return aws_session
    except (TypeError, ProfileNotFound) as e:
        logger.error("Unable to get session - {error}".format(error=e.__str__()))


def get_aws_keys_from_profile(logger, session):
    """
    Gets AWS keys from profile.

    :param logger: logger
    :param session: AWS session

    :type logger: logging.Logger 
    :type session: boto3.session.Session

    :returns: access key and secret key
    :rtype: str, str
    """
    logger.debug("Getting AWS keys.")
    return session.get_credentials().access_key, session.get_credentials().secret_key
