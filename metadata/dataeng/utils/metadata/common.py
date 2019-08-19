#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import datetime


def get_metadata(logger, storage_client, gcs_bucket, data_source, data_source_type):
    """
    Gets metadata for a data source type.

    :param logger: logger
    :param storage_client: Google Cloud Storage Client
    :param gcs_bucket: GCS bucket
    :param data_source: data source
    :param data_source_type: data source type

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type gcs_bucket: str
    :type data_source: str
    :type data_source_type: str

    :returns: metadata
    :rtype: dict
    """
    logger.debug("Getting bucket gs://{gcs_bucket} using - {client_type}.".format(gcs_bucket=gcs_bucket,
                                                                                  client_type=type(storage_client)))
    bucket = storage_client.get_bucket(gcs_bucket)
    logger.debug("Getting metadata blob for {data_source}/{data_source_type}.".format(data_source=data_source,
                                                                                      data_source_type=data_source_type))
    blob = bucket.blob("{data_source}/{data_source_type}/metadata.json".format(data_source=data_source,
                                                                               data_source_type=data_source_type))

    if not blob.exists():
        logger.debug("Metadata for {data_source}/{data_source_type} does not exist.".format(data_source=data_source,
                                                                                            data_source_type=data_source_type))
        return

    logger.debug("Downloading metadata for {data_source}/{data_source_type}.".format(data_source=data_source,
                                                                                     data_source_type=data_source_type))
    blob_string = blob.download_as_string()
    logger.debug("Decoding metadata for {data_source}/{data_source_type}.".format(data_source=data_source,
                                                                                  data_source_type=data_source_type))
    blob_string = blob_string.decode("utf-8")
    logger.debug("Loading metadata for {data_source}/{data_source_type} into a dict.".format(data_source=data_source,
                                                                                             data_source_type=data_source_type))
    metadata = json.loads(blob_string)
    return metadata


def update_metadata(logger, storage_client, gcs_bucket, data_source, data_source_type, market, format, prefix,
                    yesterday=False):
    """
    Updates metadata.

    :param logger: logger
    :param storage_client: Google Cloud Storage Client
    :param gcs_bucket: GCS bucket
    :param data_source: data source
    :param data_source_type: data source type
    :param market: market
    :param format: file format
    :param prefix: last processed prefix

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type gcs_bucket: str
    :type data_source: str
    :type data_source_type: str
    :type market: str
    :type format: str
    :type prefix: str
    """
    metadata = get_metadata(logger, storage_client, gcs_bucket, data_source, data_source_type)

    if metadata is None:
        logger.debug("Metadata for {data_source}/{data_source_type} does not exist, create a new one.".format(
            data_source=data_source, data_source_type=data_source_type))
        metadata = dict()

    if "last_success" not in metadata:
        metadata["last_success"] = dict()

    if format not in metadata["last_success"]:
        metadata["last_success"][format] = dict()

    if market not in metadata["last_success"][format]:
        metadata["last_success"][format][market] = dict()

    if not yesterday:
        metadata["last_success"][format][market]["last_updated"] = str(datetime.utcnow())
        metadata["last_success"][format][market]["prefix"] = prefix
    else:
        metadata["last_success"][format][market]["yesterday_last_updated"] = str(datetime.utcnow())
        metadata["last_success"][format][market]["yesterday_prefix"] = prefix

    metadata_string = json.dumps(metadata, indent=2)
    logger.debug("Uploading metadata: {metadata} for {data_source}/{data_source_type}.".format(metadata=metadata_string,
                                                                                               data_source=data_source,
                                                                                               data_source_type=data_source_type))
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob("{data_source}/{data_source_type}/metadata.json".format(data_source=data_source,
                                                                               data_source_type=data_source_type))
    blob.upload_from_string(metadata_string)


def update_custom_metadata(logger, storage_client, gcs_bucket, data_source, data_source_type, market, format, prefix,
                           custom_values, yesterday=False):
    """
    Updates custom metadata.

    :param logger: logger
    :param storage_client: Google Cloud Storage Client
    :param gcs_bucket: GCS bucket
    :param data_source: data source
    :param data_source_type: data source type
    :param market: market
    :param format: file format
    :param prefix: last processed prefix
    :param custom_values: custom values
    :param yesterday: is yesterday

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type gcs_bucket: str
    :type data_source: str
    :type data_source_type: str
    :type market: str
    :type format: str
    :type prefix: str
    :type custom_values: dict
    :type yesterday: bool
    """
    metadata = get_metadata(logger, storage_client, gcs_bucket, data_source, data_source_type)

    if metadata is None:
        logger.debug("Metadata for {data_source}/{data_source_type} does not exist, create a new one.".format(
            data_source=data_source, data_source_type=data_source_type))
        metadata = dict()

    if "last_success" not in metadata:
        metadata["last_success"] = dict()

    if format not in metadata["last_success"]:
        metadata["last_success"][format] = dict()

    if market not in metadata["last_success"][format]:
        metadata["last_success"][format][market] = dict()

    if not yesterday:
        metadata["last_success"][format][market]["last_updated"] = str(datetime.utcnow())
        metadata["last_success"][format][market]["prefix"] = prefix
    else:
        metadata["last_success"][format][market]["yesterday_last_updated"] = str(datetime.utcnow())
        metadata["last_success"][format][market]["yesterday_prefix"] = prefix

    if custom_values:
        metadata["last_success"][format][market]["custom_values"] = custom_values

    metadata_string = json.dumps(metadata, indent=2)
    logger.debug("Uploading metadata: {metadata} for {data_source}/{data_source_type}.".format(metadata=metadata_string,
                                                                                               data_source=data_source,
                                                                                               data_source_type=data_source_type))
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob("{data_source}/{data_source_type}/metadata.json".format(data_source=data_source,
                                                                               data_source_type=data_source_type))
    blob.upload_from_string(metadata_string)


def remove_yesterday_metdata(logger, storage_client, gcs_bucket, data_source, data_source_type, market, format):
    """
    Removes yesterday's metedata.

    :param logger: logger
    :param storage_client: Google Cloud Storage Client
    :param gcs_bucket: GCS bucket
    :param data_source: data source
    :param data_source_type: data source type
    :param market: market
    :param format: file format

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type gcs_bucket: str
    :type data_source: str
    :type data_source_type: str
    :type market: str
    :type format: str
    """
    metadata = get_metadata(logger, storage_client, gcs_bucket, data_source, data_source_type)

    if metadata is None:
        return

    if "last_success" not in metadata:
        metadata["last_success"] = dict()

    if format not in metadata["last_success"]:
        metadata["last_success"][format] = dict()

    if market not in metadata["last_success"][format]:
        metadata["last_success"][format][market] = dict()

    if "yesterday_last_updated" in metadata["last_success"][format][market]:
        del metadata["last_success"][format][market]["yesterday_last_updated"]

    if "yesterday_prefix" in metadata["last_success"][format][market]:
        del metadata["last_success"][format][market]["yesterday_prefix"]

    metadata_string = json.dumps(metadata, indent=2)
    logger.debug("Removing yesterday's metadata for {data_source}/{data_source_type}.".format(data_source=data_source,
                                                                                              data_source_type=data_source_type))
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob("{data_source}/{data_source_type}/metadata.json".format(data_source=data_source,
                                                                               data_source_type=data_source_type))
    blob.upload_from_string(metadata_string)


def mark_success(logger, storage_client, gcs_bucket, success_file_location):
    """
    Marks _SUCCESS file.

    :param logger: logger
    :param storage_client: Google Cloud Storage Client
    :param gcs_bucket: GCS bucket
    :param success_file_location: success file location

    :type logger: logging.Logger
    :type storage_client: google.cloud.storage.client.Client
    :type gcs_bucket: str
    :type success_file_location: str
    """
    logger.debug("Marking _SUCCESS file.")
    dt = datetime.utcnow()
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(success_file_location)
    content = str(dt)
    blob.upload_from_string(content)
    logger.debug("_SUCCESS file has been uploaded with content: {content}.".format(content=content))
