#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BufferedReader, BufferedWriter

import boto3
import paramiko

from boto3.exceptions import S3UploadFailedError
from botocore.config import Config
from botocore.exceptions import NoRegionError, ClientError
from dataeng.utils.data_type import represent_int


def get_s3_client(logger, session, config=None):
    """
    Gets an instance of S3 client.

    :param logger: logger
    :param session: AWS session
    :param config: Boto config

    :type logger: logging.Logger
    :type session: boto3.session.Session
    :type: config: botocore.config.Config

    :returns: S3 client
    :rtype: botocore.client.S3
    """
    if not config:
        config = Config(retries=dict(max_attempts=10))

    try:
        logger.debug("Getting S3 client.")
        return session.client("s3", config=config)
    except NoRegionError as e:
        logger.error("Unable to get S3 client - {error}".format(error=e.__str__()))


def get_s3_resource(logger, session, config=None):
    """
    Gets an instance of S3 resource.

    :param logger: logger
    :param session: AWS session
    :param config: Boto config

    :type logger: logging.Logger
    :type session: boto3.session.Session
    :type: config: botocore.config.Config

    :returns: S3 resource
    :rtype: boto3.resources.factory.s3.ServiceResource
    """
    if not config:
        config = Config(retries=dict(max_attempts=10))

    try:
        logger.debug("Getting S3 resource.")
        return session.resource("s3", config=config)
    except NoRegionError as e:
        logger.error("Unable to get S3 resource - {error}".format(error=e.__str__()))


def is_s3_key_exists(logger, s3_resource, s3_bucket, s3_key):
    """
    Checks the existence of S3 key.

    :param logger: logger
    :param s3_resource: S3 resource
    :param s3_bucket: S3 bucket
    :param s3_key: S3 key

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type s3_bucket: str
    :type s3_key: str

    :returns: True if key exists, False if key does not exist, None if unable to determine
    :rtype: bool
    """
    try:
        logger.debug("Checking the existence of s3://{s3_bucket}/{s3_key}".format(s3_bucket=s3_bucket, s3_key=s3_key))
        objs = list(s3_resource.Bucket(s3_bucket).objects.filter(Prefix=s3_key))
        return len(objs) > 0 and objs[0].key == s3_key
    except ClientError as e:
        if represent_int(e.response["Error"]["Code"]):
            error_code = int(e.response["Error"]["Code"])

            if error_code == 403:
                logger.debug("Bucket {s3_bucket} is forbidden".format(s3_bucket=s3_bucket))
                return False

        logger.warning("Unable to determine S3 key {s3_key} - {error}".format(s3_key=s3_key, error=e.__str__()))


def is_s3_bucket_exists(logger, s3_resource, s3_bucket):
    """
    Checks the existence of S3 bucket.

    :param logger: logger
    :param s3_resource: S3 resource
    :param s3_bucket: S3 bucket

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type s3_bucket: str

    :returns: True if bucket exists, False if bucket does not exist or forbidden, None if unable to determine
    :rtype: bool
    """
    try:
        logger.debug("Checking the existence of s3://{s3_bucket}".format(s3_bucket=s3_bucket))
        s3_resource.meta.client.head_bucket(Bucket=s3_bucket)
        return True
    except ClientError as e:
        if represent_int(e.response["Error"]["Code"]):
            error_code = int(e.response["Error"]["Code"])

            if error_code == 403:
                logger.debug("Bucket {s3_bucket} is forbidden".format(s3_bucket=s3_bucket))
                return False
            elif error_code == 404:
                logger.debug("Bucket {s3_bucket} does not exist".format(s3_bucket=s3_bucket))
                return False

        logger.warning(
            "Unable to determine S3 bucket {s3_bucket} - {error}".format(s3_bucket=s3_bucket, error=e.__str__()))


def upload_s3_object(logger, s3_resource, f, s3_bucket, s3_key, callback=None):
    """
    Uploads file object to S3.

    :param logger: logger
    :param s3_resource: S3 resource
    :param f: file path or file object
    :param s3_bucket: S3 bucket
    :param s3_key: S3 key
    :param callback: callback for monitoring progress

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type f: str | io.BufferedReader | paramiko.SFTPFile
    :type s3_bucket: str 
    :type s3_key: str
    :type callback: typing.Callables
    """
    try:
        if isinstance(f, str):
            logger.debug(
                "Uploading {f} to s3://{s3_bucket}/{s3_key}".format(f=f, s3_bucket=s3_bucket, s3_key=s3_key))
            s3_resource.meta.client.upload_file(f, s3_bucket, s3_key, Callback=callback)
        elif isinstance(f, BufferedReader) or isinstance(f, paramiko.SFTPFile):
            logger.debug(
                "Uploading a file object to s3://{s3_bucket}/{s3_key}".format(s3_bucket=s3_bucket, s3_key=s3_key))
            s3_resource.meta.client.upload_fileobj(f, s3_bucket, s3_key, Callback=callback)
        else:
            logger.error("Invalid input type for upload - {input_type}".format(input_type=type(f).__name__))
    except S3UploadFailedError as e:
        logger.warning(
            "Unable to upload file to s3://{s3_bucket}/{s3_key} - {error}".format(s3_bucket=s3_bucket, s3_key=s3_key,
                                                                                  error=e.__str__()))


def download_s3_object(logger, s3_resource, f, s3_bucket, s3_key, callback=None):
    """
    Downloads file object from S3.

    :param logger: logger
    :param s3_resource: S3 resource
    :param f: file path or file object
    :param s3_bucket: S3 bucket
    :param s3_key: S3 key
    :param callback: callback for monitoring progress

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type f: str | io.BufferedWriter
    :type s3_bucket: str 
    :type s3_key: str
    :type callback: typing.Callables
    """
    try:
        if isinstance(f, str):
            logger.debug("Downloading s3://{s3_bucket}/{s3_key} to {f}".format(s3_bucket=s3_bucket, s3_key=s3_key, f=f))
            s3_resource.meta.client.download_file(s3_bucket, s3_key, f, Callback=callback)
        elif isinstance(f, BufferedWriter):
            logger.debug(
                "Downloading s3://{s3_bucket}/{s3_key} to a file object".format(s3_bucket=s3_bucket, s3_key=s3_key))
            s3_resource.meta.client.download_fileobj(s3_bucket, s3_key, f, Callback=callback)
        else:
            logger.error("Invalid input type for download - {input_type}".format(input_type=type(f).__name__))
    except ClientError as e:
        if represent_int(e.response["Error"]["Code"]):
            error_code = int(e.response["Error"]["Code"])

            if error_code == 403:
                logger.debug(
                    "S3 object s3://{s3_bucket}/{s3_key} is forbidden".format(s3_bucket=s3_bucket, s3_key=s3_key))
                return
            elif error_code == 404:
                logger.debug(
                    "S3 object s3://{s3_bucket}/{s3_key} does not exist".format(s3_bucket=s3_bucket, s3_key=s3_key))
                return

        logger.warning(
            "Unable to download s3://{s3_bucket}/{s3_key} - {error}".format(s3_bucket=s3_bucket, s3_key=s3_key,
                                                                            error=e.__str__()))


def delete_s3_object(logger, s3_resource, s3_bucket, s3_key):
    """
    Deletes an S3 object.

    :param logger: logger
    :param s3_resource: S3 resource
    :param s3_bucket: S3 bucket
    :param s3_key: S3 key

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type s3_bucket: str
    :type s3_key: str
    """
    try:
        logger.debug("Deleting s3://{s3_bucket}/{s3_key}".format(s3_bucket=s3_bucket, s3_key=s3_key))
        s3_resource.Object(s3_bucket, s3_key).delete()
    except ClientError as e:
        logger.warning("Unable to delete S3 key {s3_key} - {error}".format(s3_key=s3_key, error=e.__str__()))


def delete_s3_prefix(logger, s3_resource, s3_bucket, s3_prefix):
    """
    Deletes an S3 prefix.

    :param logger: logger
    :param s3_resource: S3 resource
    :param s3_bucket: S3 bucket
    :param s3_prefix: S3 prefix

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type s3_bucket: str
    :type s3_prefix: str
    """
    try:
        logger.debug("Deleting s3://{s3_bucket}/{s3_prefix}/".format(s3_bucket=s3_bucket, s3_prefix=s3_prefix))
        s3_resource.Bucket(s3_bucket).objects.filter(Prefix=s3_prefix).delete()
        s3_resource.Object(bucket_name=s3_bucket, key=s3_prefix).delete()
    except ClientError as e:
        if represent_int(e.response["Error"]["Code"]):
            error_code = int(e.response["Error"]["Code"])

            if error_code == 403:
                logger.debug(
                    "s3://{s3_bucket}/{s3_prefix}/ is forbidden".format(s3_bucket=s3_bucket, s3_prefix=s3_prefix))
                return
            elif error_code == 404:
                logger.debug(
                    "s3://{s3_bucket}/{s3_prefix}/ does not exist".format(s3_bucket=s3_bucket, s3_prefix=s3_prefix))
                return

        logger.warning("Unable to delete S3 prefix s3://{s3_bucket}/{s3_prefix} - {error}".format(s3_bucket=s3_bucket,
                                                                                                  s3_prefix=s3_prefix,
                                                                                                  error=e.__str__()))


def list_s3_keys(logger, s3_resource, s3_bucket, s3_prefix="", search="Contents"):
    """
    Lists S3 keys for a given S3 bucket and S3 prefix.

    :param logger: logger
    :param s3_resource: S3 resource
    :param s3_bucket: S3 bucket
    :param s3_prefix: S3 prefix
    :param search: JMESPath search string

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type s3_bucket: str
    :type s3_prefix: str
    :type search: str

    :returns: list of S3 keys
    :rtype: list
    """
    try:
        if s3_prefix and s3_prefix[0] == "/":
            s3_prefix = s3_prefix[1:]

        if s3_prefix and s3_prefix[len(s3_prefix) - 1] != "/":
            s3_prefix = "{s3_prefix}/".format(s3_prefix=s3_prefix)

        paginator = s3_resource.meta.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

        if search is not None:
            page_iterator = page_iterator.search(search)

        s3_keys = []

        for key_data in page_iterator:
            if key_data is not None:
                if not key_data["Key"].endswith("/"):
                    s3_keys.append(key_data["Key"])

        return s3_keys
    except ClientError as e:
        if represent_int(e.response["Error"]["Code"]):
            error_code = int(e.response["Error"]["Code"])

            if error_code == 403:
                logger.debug(
                    "s3://{s3_bucket}/{s3_prefix} is forbidden".format(s3_bucket=s3_bucket, s3_prefix=s3_prefix))
                return
            elif error_code == 404:
                logger.debug(
                    "s3://{s3_bucket}/{s3_prefix} does not exist".format(s3_bucket=s3_bucket, s3_prefix=s3_prefix))
                return

        logger.warning("Unable to list S3 keys from s3://{s3_bucket}/{s3_prefix} - {error}".format(s3_bucket=s3_bucket,
                                                                                                   s3_prefix=s3_prefix,
                                                                                                   error=e.__str__()))


def get_s3_object_size(logger, s3_resource, s3_bucket, s3_key):
    """
    Gets S3 object size.

    :param logger: logger
    :param s3_resource: S3 resource
    :param s3_bucket: S3 bucket
    :param s3_prefix: S3 prefix
    :param s3_keys: list of S3 keys

    :type logger: logging.Logger
    :type s3_resource: boto3.resources.factory.s3.ServiceResource
    :type s3_bucket: str
    :type s3_prefix: str
    :type s3_keys: list

    :returns: size of S3 object in bytes
    :rtype: int
    """
    try:
        return s3_resource.meta.client.head_object(Bucket=s3_bucket, Key=s3_key)["ContentLength"]
    except ClientError as e:
        if represent_int(e.response["Error"]["Code"]):
            error_code = int(e.response["Error"]["Code"])

            if error_code == 403:
                logger.debug("s3://{s3_bucket}/{s3_key} is forbidden".format(s3_bucket=s3_bucket, s3_key=s3_key))
                return
            elif error_code == 404:
                logger.debug("s3://{s3_bucket}/{s3_key} does not exist".format(s3_bucket=s3_bucket, s3_key=s3_key))
                return

        logger.warning("Unable to determine get size of s3://{s3_bucket}/{s3_key} - {error}".format(s3_bucket=s3_bucket,
                                                                                                    s3_key=s3_key,
                                                                                                    error=e.__str__()))
