#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib

import re
import uuid

import requests

DOMAIN_NAME = "dataeng.com"
DAGS_BUCKET_PREFIX = "composer"
DATAFLOW_BUCKET_PREFIX = "dataflow"
TEST_BUCKET_PREFIX = "test"
DAGS_PREFIX = "dags"


def get_ingestion_bucket_name(project_id, org_name, group_name):
    """
    Gets ingestion bucket name.

    :param project_id: GCP project ID
    :param org_name: organisation name
    :param group_name: business group name

    :type project_id: str
    :type org_name: str
    :type group_name: str

    :returns: ingestion bucket name
    :rtype: str
    """
    hash_sha1 = hashlib.sha1()
    org_name = org_name.lower()
    group_name = group_name.lower()
    text = "{project_id}{org_name}{group_name}".format(project_id=project_id, org_name=org_name, group_name=group_name)
    hash_sha1.update(text.encode("utf-8"))
    bucket_name = "{hashed}.{domain_name}".format(hashed=hash_sha1.hexdigest()[:10], domain_name=DOMAIN_NAME)
    return bucket_name


def get_modeling_bucket_name(project_id, org_name, group_name, repo_name):
    """
    Gets modeling bucket name.

    :param project_id: GCP project ID
    :param org_name: organisation name
    :param group_name: business group name
    :param repo_name: DataFlow Git repository name

    :type project_id: str
    :type org_name: str
    :type group_name: str
    :type repo_name: str

    :returns: modeling bucket name
    :rtype: str
    """
    hash_sha1 = hashlib.sha1()
    org_name = org_name.lower()
    group_name = group_name.lower()
    repo_name = repo_name.lower()
    text = "{project_id}{org_name}{group_name}{repo_name}".format(project_id=project_id, org_name=org_name,
                                                                  group_name=group_name, repo_name=repo_name)
    hash_sha1.update(text.encode("utf-8"))
    bucket_name = "{hashed}.{domain_name}".format(hashed=hash_sha1.hexdigest()[:10], domain_name=DOMAIN_NAME)
    return bucket_name


def get_dags_bucket_name(project_id):
    """
    Gets a DAGs bucket name.

    :param project_id:

    :type project_id: str

    :returns: DAGs bucket name
    :rtype: str
    """
    hash_sha1 = hashlib.sha1()
    hash_sha1.update(project_id.encode("utf-8"))
    bucket_name = "{bucket_prefix}-{hashed}.{domain_name}".format(bucket_prefix=DAGS_BUCKET_PREFIX,
                                                                  hashed=hash_sha1.hexdigest()[:10],
                                                                  domain_name=DOMAIN_NAME)
    return bucket_name


def get_dataflow_bucket_name(project_id):
    """
    Gets a DataFlow staging bucket name.

    :param project_id:

    :type project_id: str

    :returns: DataFlow staging bucket name
    :rtype: str
    """
    hash_sha1 = hashlib.sha1()
    hash_sha1.update(project_id.encode("utf-8"))
    bucket_name = "{bucket_prefix}-{hashed}.{domain_name}".format(bucket_prefix=DATAFLOW_BUCKET_PREFIX,
                                                                  hashed=hash_sha1.hexdigest()[:10],
                                                                  domain_name=DOMAIN_NAME)
    return bucket_name


def get_test_bucket_name(project_id):
    """
    Gets a test bucket name.

    :param project_id:

    :type project_id: str

    :returns: test bucket name
    :rtype: str
    """
    hash_sha1 = hashlib.sha1()
    hash_sha1.update(project_id.encode("utf-8"))
    bucket_name = "{bucket_prefix}-{hashed}.{domain_name}".format(bucket_prefix=TEST_BUCKET_PREFIX,
                                                                  hashed=hash_sha1.hexdigest()[:10],
                                                                  domain_name=DOMAIN_NAME)
    return bucket_name


def get_dags_location(dags_bucket_name):
    """
    Gets DAGs location.

    :param dags_bucket_name: DAGs bucket name

    :type dags_bucket_name: str

    :returns: DAGs location on GCS
    :rtype: str
    """
    return "gs://{dags_bucket_name}/{dags_prefix}".format(dags_bucket_name=dags_bucket_name, dags_prefix=DAGS_PREFIX)


def get_prefix(data_source, data_source_type, format, location="_", year="_", month="_", day="_", hour="_", minute="_",
               second="_"):
    """
    Gets prefix for a data source or a model. Note that, there is no validation of appropriate timestamp portions.
    
    :param data_source: data source name or model name
    :param data_source_type : data source type
    :param format: format
    :param location: location
    :param year: year
    :param month: month
    :param day: day
    :param hour: hour
    :param minute: minute
    :param second: second

    :type data_source: str
    :type data_source_type : str
    :type format: str
    :type location: str
    :type year: str
    :type month: str
    :type day: str
    :type hour: str
    :type minute: str
    :type second: str

    :returns: prefix
    :rtype: str
    """
    prefix = "{data_source}/{data_source_type}/{location}/{year}/{month}/{day}/{hour}/{minute}/{second}/{format}/".format(
        data_source=data_source, data_source_type=data_source_type, location=location, year=_zfill_with_len(year, 4),
        month=_zfill_with_len(month, 2), day=_zfill_with_len(day, 2), hour=_zfill_with_len(hour, 2),
        minute=_zfill_with_len(minute, 2), second=_zfill_with_len(second, 2), format=format)
    return prefix


def get_project_id():
    """
    Gets GCP project ID.

    :returns: GCP project ID
    :rtype: str
    """
    metadata_server = "http://metadata/computeMetadata/v1/project"
    metadata_flavor = {"Metadata-Flavor": "Google"}
    url = "{metadata_server}/project-id".format(metadata_server=metadata_server)
    project_id = requests.get(url, headers=metadata_flavor).text
    return project_id


def get_staging_dataset_id(data_source, data_source_type):
    """
    Gets staging BigQuery dataset ID.

    :param data_source: data source
    :param data_source_type: data source type

    :type data_source: str
    :type data_source_type: str

    :returns: staging dataset ID
    :rtype: str
    """
    pattern = re.compile(r"[\W]")
    data_source = pattern.sub("_", data_source)
    data_source = data_source.strip("_")
    data_source_type = pattern.sub("_", data_source_type)
    data_source_type = data_source_type.strip("_")
    random_string = str(uuid.uuid4()).replace("-", "_")
    return "staging_{data_source}_{data_source_type}_{random_string}".format(data_source=data_source,
                                                                             data_source_type=data_source_type,
                                                                             random_string=random_string)


def _zfill_with_len(val, len):
    """
    Returns the appropriate place holder for a given length

    :param val: value
    :param len: expected length of value

    :type val: str
    :type len: int

    :returns: formatted value
    :rtype: str
    """
    if val == "_" or val is None:
        return val
    else:
        return val.zfill(len)


def get_success_file_location(data_source, data_source_type):
    """
    Gets success file location.

    :param data_source: data source name or model name
    :param data_source_type: data source type

    :type data_source: str
    :type data_source_type: str

    :returns: success file location
    :rtype: str
    """
    success_file_location = "{data_source}/{data_source_type}/_SUCCESS".format(data_source=data_source,
                                                                               data_source_type=data_source_type)
    return success_file_location
