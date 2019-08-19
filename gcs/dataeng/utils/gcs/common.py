#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BufferedReader

import paramiko
from google.cloud import storage
from google.api_core.exceptions import AlreadyExists, NotFound, Forbidden, Conflict


def get_storage_client(logger):
    """
    Gets a google cloud storage client.

    :param logger: logger

    :type logger: logging.Logger

    :returns:Google Cloud Storage Client
    :rtype: google.cloud.storage.client.Client
    """
    try:
        logger.debug("Getting GCS resource.")
        return storage.Client()
    except Exception as e:
        logger.error("Unable to get GCS client - {error}".format(error=e.__str__()))


def create_bucket(logger, storage_client, bucket_name):
    """
    Creates and returns a GCS bucket.

    :param logger: logger
    :param storage_client: GCS storage client
    :param bucket_name : Name of the bucket

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type bucket_name: str

    :returns: Bucket in GCP with the name mentioned
    :rtype: google.cloud.storage.bucket
    """
    try:
        bucket = storage_client.create_bucket(bucket_name)
        logger.info('Bucket {} created'.format(bucket_name))
        return bucket
    except Forbidden as e:
        logger.warning(e.__str__())
    except AlreadyExists as e:
        logger.debug(e.__str__())
        return storage_client.get_bucket(bucket_name)


def delete_bucket(logger, storage_client, bucket_name, force=False):
    """
    Deletes a bucket in GCP.

    :param logger: logger
    :param storage_client: GCS storage client
    :param bucket_name : Name of the bucket

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type bucket_name : str
    """
    try:
        logger.debug("Deleting gs://{bucket_name}".format(bucket_name=bucket_name))
        bucket = storage_client.bucket(bucket_name)
        bucket.delete(force=force)
    except (NotFound, Forbidden, Conflict)as e:
        logger.warning(e.__str__())


def get_bucket_labels(logger, storage_client, bucket_name):
    """
    Deletes a bucket in GCP
    :param logger: logger
    :param storage_client : Google Cloud Storage Client
    :param bucket_name : Name of the bucket

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type bucket_name : str

    :returns: Labels of a bucket
    :rtype: Dictionary
    """
    try:
        bucket = storage_client.get_bucket(bucket_name)
        labels = bucket.labels
        return labels
    except (NotFound, Forbidden) as e:
        logger.warning(e.__str__)


def add_bucket_label(logger, storage_client, bucket_name, key, value):
    """
    Adds a label to a bucket.

    :param logger: logger
    :param storage_client: GCS storage client
    :param bucket_name : Name of the bucket
    :param key : key of the label
    :param value : value of the label

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type bucket_name : str
    :type key : str
    :type value : str
    """
    try:
        bucket = storage_client.get_bucket(bucket_name)
        labels = bucket.labels
        labels[key] = value
        bucket.labels = labels
        bucket.patch()
        logger.debug("Updated labels for gs://{bucket_name}.".format(bucket_name=bucket_name))
    except (NotFound, Forbidden) as e:
        logger.warning(e.__str__)


def is_gcs_key_exists(logger, storage_client, gcs_bucket, gcs_key):
    """
    Checks the existence of GCS key.

    :param logger: logger
    :param storage_client: GCS storage client
    :param gcs_bucket: GCS bucket
    :param gcs_key: GCS key

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.Client
    :type gcs_bucket: str
    :type gcs_key: str

    :returns: True if key exists, False if key does not exist, None if unable to determine
    :rtype: bool
    """
    logger.debug("Checking the existence of gs://{gcs_bucket}/{gcs_key}".format(gcs_bucket=gcs_bucket, gcs_key=gcs_key))
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_key)
    return blob.exists()


def delete_gcs_object(logger, storage_client, gcs_bucket, gcs_key):
    """
    Deletes GCS object.

    :param logger: logger
    :param storage_client: GCS storage client
    :param gcs_bucket: GCS bucket
    :param gcs_key: GCS key

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.Client
    :type gcs_bucket: str
    :type gcs_key: str
    """
    logger.debug("Deleting gs://{gcs_bucket}/{gcs_key}.".format(gcs_bucket=gcs_bucket, gcs_key=gcs_key))
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_key)
    blob.delete()


def delete_gcs_prefix(logger, storage_client, gcs_bucket, gcs_prefix):
    """
    Deletes all objects in GCS prefix.

    :param logger: logger
    :param storage_client: GCS storage client
    :param gcs_bucket: GCS bucket
    :param gcs_prefix: GCS prefix

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.Client
    :type gcs_bucket: str
    :type gcs_prefix: str
    """
    logger.info("Deleting gs://{gcs_bucket}/{gcs_prefix}".format(gcs_bucket=gcs_bucket, gcs_prefix=gcs_prefix))
    bucket = storage_client.get_bucket(gcs_bucket)
    blobs = bucket.list_blobs(prefix=gcs_prefix)

    for blob in blobs:
        blob.delete()


def upload_object_to_gcs(logger, gcs_storage_client, f, gcs_bucket, gcs_key):
    """
    Uploads file object to GCS. Note that, currently this method supports only uploading from a file.

    :param logger: logger
    :param gcs_storage_client: GCS storage client
    :param f: file path or file object
    :param gcs_bucket: GCS bucket
    :param gcs_key: GCS key

    :type logger: logging.Logger
    :type gcs_storage_client: google.cloud.storage.client.Client
    :type f: str | io.BufferedReader | paramiko.SFTPFile
    :type gcs_bucket: str
    :type gcs_key: str
    """
    try:
        bucket = gcs_storage_client.get_bucket(gcs_bucket)
        blob = bucket.blob(gcs_key)

        if isinstance(f, str):
            logger.debug(
                "Uploading {file_name} to gs://{gcs_bucket}/{gcs_key}.".format(file_name=f, gcs_bucket=gcs_bucket,
                                                                               gcs_key=gcs_key))
            blob.upload_from_filename(f)
        elif isinstance(f, BufferedReader) or isinstance(f, paramiko.SFTPFile):
            logger.debug(
                "Uploading a file object to gs://{gcs_bucket}/{gcs_key}.".format(gcs_bucket=gcs_bucket,
                                                                                 gcs_key=gcs_key))
            blob.upload_from_file(f)
        else:
            logger.error("Invalid input type for upload - {input_type}".format(input_type=type(f).__name__))
            return

        logger.info(
            "File {file_name} uploaded to gs://{gcs_bucket}/{gcs_key}.".format(file_name=f, gcs_bucket=gcs_bucket,
                                                                               gcs_key=gcs_key))
    except NotFound as e:
        logger.info("gs://{gcs_bucket} does not exist - {error}.".format(gcs_bucket=gcs_bucket, error=e.__str__()))


def download_object_from_gcs(logger, file_name, gcs_storage_client, gcs_bucket, gcs_key):
    """
    Downloads object from GCS.

    :param logger: logger
    :param file_name: destination filename
    :param gcs_storage_client: GCS storage client
    :param gcs_bucket: GCS bucket
    :param gcs_key: GCS key

    :type logger: logging.Logger
    :type file_name: str
    :type gcs_storage_client: google.cloud.storage.client.Client
    :type gcs_bucket: str
    :type gcs_key: str
    """
    try:
        logger.debug(
            "Downloading: gs://{gcs_bucket}/{gcs_key} onto {file_name}".format(gcs_bucket=gcs_bucket, gcs_key=gcs_key,
                                                                               file_name=file_name))
        bucket = gcs_storage_client.get_bucket(gcs_bucket)
        blob = bucket.blob(gcs_key)
        blob.download_to_filename(file_name)
    except NotFound as e:
        logger.info("gs://{gcs_bucket}/{gcs_key} not found - {error}.".format(gcs_bucket=gcs_bucket, gcs_key=gcs_key,
                                                                              error=e.__str__()))
