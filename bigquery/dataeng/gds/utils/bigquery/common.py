#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import tempfile
from datetime import datetime
from json import JSONDecodeError

import google.auth
from google.api_core.exceptions import AlreadyExists, BadRequest, NotFound, Conflict
from google.cloud import bigquery


def get_bq_client(logger, creds=None):
    """
    Gets a BigQuery client.

    :param logger: logger
    :param creds: path to or a dict of Google application credentials

    :type logger: logging.Logger
    :type creds: str | dict

    :returns: BigQuery client
    :rtype: google.cloud.bigquery.client.Client
    """
    try:
        if creds is None:
            credentials, _ = google.auth.default()
            return bigquery.Client(credentials=credentials)

        if isinstance(creds, str):
            return bigquery.Client.from_service_account_json(creds)
        elif isinstance(creds, dict):
            temp = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")

            try:
                temp.write(json.dumps(creds, indent=2))
                temp.flush()
                os.fsync(temp.fileno())
                return bigquery.Client.from_service_account_json(temp.name)
            finally:
                temp.close()
        else:
            logger.error("Invalid credentials type {creds_type}.".format(creds_type=type(creds)))
    except (JSONDecodeError, AttributeError, FileNotFoundError, ValueError, OSError) as e:
        logger.error(e.__str__())


def create_bq_dataset(logger, bq_client, location, dataset_id, project_id=None):
    """
    Create a BigQuery dataset.

    :param logger: logger
    :param bq_client: BigQuery client
    :param location: data location
    :param dataset_id: dataset ID
    :param project_id: project ID

    :type logger: logging.Logger
    :type bq_client: google.cloud.bigquery.client.Client
    :type location: str
    :type dataset_id: str
    :type project_id: str

    :returns: dataset
    :rtype: google.cloud.bigquery.dataset.Dataset
    """
    try:
        if not project_id:
            project_id = bq_client.project

        logger.info("Creating dataset {project_id}.{dataset_id} in {location}.".format(project_id=project_id,
                                                                                       dataset_id=dataset_id,
                                                                                       location=location))
        dataset = bigquery.Dataset(bq_client.dataset(dataset_id, project_id))
        dataset.location = location
        return bq_client.create_dataset(dataset)
    except AlreadyExists as e:
        logger.debug(e.__str__())
    except (BadRequest, Conflict) as e:
        logger.error(e.__str__())


def delete_bq_dataset(logger, bq_client, dataset_id, delete_contents=False, project_id=None):
    """
    Deletes a BigQuery dataset.

    :param logger: logger
    :param bq_client: BigQuery client
    :param dataset_id: dataset ID
    :param delete_contents: whether to delete datasets with contents in it
    :param project_id: project ID

    :type logger: logging.Logger
    :type bq_client: google.cloud.bigquery.client.Client
    :type dataset_id: str
    :type delete_contents: bool
    :type project_id: str
    """
    try:
        if not project_id:
            project_id = bq_client.project

        logger.info("Deleting dataset {project_id}.{dataset_id}.".format(project_id=project_id, dataset_id=dataset_id))
        dataset_ref = bq_client.dataset(dataset_id, project_id)
        bq_client.delete_dataset(dataset_ref, delete_contents=delete_contents)
    except NotFound as e:
        logger.debug(e.__str__())
    except BadRequest as e:
        logger.error(e.__str__())


def create_bq_table(logger, bq_client, dataset_id, table_id, gcp_schema, ignore_partitioning=False, project_id=None):
    """
    Creats a BigQuery table.

    :param logger: logger
    :param bq_client: BigQuery client
    :param dataset_id: dataset ID
    :param table_id: table ID
    :param gcp_schema: schema
    :param ignore_partitioning: ignore partitioning
    :param project_id: project ID

    :type logger: logging.Logger
    :type bq_client: google.cloud.bigquery.client.Client
    :type dataset_id: str
    :type table_id: str
    :type gcp_schema: list
    :type ignore_partitioning: bool
    :type project_id: str

    :returns: table
    :rtype: google.cloud.bigquery.table.Table
    """
    try:
        if not project_id:
            project_id = bq_client.project

        table_ref = bq_client.dataset(dataset_id, project_id).table(table_id)
        table = bigquery.Table(table_ref, schema=gcp_schema["schema"])

        if "time_partitioning" in gcp_schema and not ignore_partitioning:
            table.time_partitioning = gcp_schema["time_partitioning"]

        logger.info("Creating {dataset_id}.{table_id}".format(dataset_id=dataset_id, table_id=table_id))
        return bq_client.create_table(table)
    except ValueError as e:
        logger.error(e.__str__())


def delete_bq_table(logger, bq_client, dataset_id, table_id, project_id=None):
    """
    Deletes a table from a dataset.

    :param logger: loggers
    :param bq_client: BigQuery client
    :param dataset_id: dataset ID
    :param table_id: table ID
    :param project_id: project ID

    :type logger: logging.Logger
    :type bq_client: google.cloud.bigquery.client.Client
    :type dataset_id: str
    :type table_id: str
    :type project_id: str
    """
    try:
        if not project_id:
            project_id = bq_client.project

        dataset_ref = bq_client.dataset(dataset_id, project_id)
        table_ref = dataset_ref.table(table_id)
        logger.info("Deleting {project_id}.{dataset_id}.{table_id}".format(project_id=project_id, dataset_id=dataset_id,
                                                                           table_id=table_id))
        bq_client.delete_table(table_ref)
    except NotFound as e:
        logger.error(e.__str__())


def load_gcs_to_bq(logger, bq_client, format, gcs_location, dataset_id, table_id, project_id=None,
                   write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, schema=None, autodetect=False,
                   time_partitioning=None, skip_leading_rows=None, location=None, use_avro_logical_types=False):
    """
    Loads data from GCS into BigQuery table.

    :param logger: logger
    :param bq_client: BigQuery client
    :param format: file format
    :param gcs_location: GCS location for the files
    :param dataset_id: dataset ID
    :param table_id: table ID
    :param project_id: project ID for BigQuery
    :param write_disposition: mode to write to BigQuery
    :param schema: schema of the table
    :param autodetect: whether to use schema autodetect feature
    :param time_partitioning: time partitioning field
    :param use_avro_logical_types: use AVRO's logical types

    :type logger: logging.Logger
    :type bq_client: google.cloud.bigquery.client.Client
    :type format: str
    :type gcs_location: str
    :type dataset_id: str
    :type table_id: str
    :type project_id: str
    :type write_disposition: str
    :type schema: list
    :type autodetect: bool
    :type time_partitioning: str
    :type use_avro_logical_types: bool
    """
    if not project_id:
        project_id = bq_client.project

    logger.info("Loading {format} into {project_id}.{dataset_id}.{table_id} from {gcs_location}".format(
        format=format, project_id=project_id, dataset_id=dataset_id, table_id=table_id, gcs_location=gcs_location))
    start_time = datetime.utcnow()
    dataset_ref = bq_client.dataset(dataset_id, project_id)
    job_config = bigquery.LoadJobConfig()

    if schema:
        job_config.schema = schema

    if time_partitioning:
        job_config.time_partitioning = time_partitioning

    if skip_leading_rows:
        job_config.skip_leading_rows = skip_leading_rows

    job_config.autodetect = autodetect
    job_config.source_format = format
    job_config.write_disposition = write_disposition
    job_config.use_avro_logical_types = use_avro_logical_types

    try:
        load_job = bq_client.load_table_from_uri(
            gcs_location,
            dataset_ref.table(table_id),
            location=location,
            job_config=job_config)
        load_job.result()
        end_time = datetime.utcnow()
        logger.info("Loading data into BigQuery took {t}".format(t=end_time - start_time))
    except NotFound as e:
        logger.warning(e.__str__())
