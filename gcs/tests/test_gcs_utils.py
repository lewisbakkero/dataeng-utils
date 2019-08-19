#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import unittest

from google.cloud import storage

from dataeng.utils.gcs import get_bucket_labels, create_bucket, add_bucket_label, delete_bucket
from dataeng.utils.logging import get_logger
from dataeng.utils.naming import get_ingestion_bucket_name


class GCSUtilTest(unittest.TestCase):
    def setUp(self):
        log_name = "test-common-utils"
        self._logger = get_logger(log_name, log_level=logging.DEBUG)
        self._client = storage.Client()
        bucket_name = get_ingestion_bucket_name("test_proj", "test_org", "test_grp")
        self._bucket_name = self._mock_bucket_name(bucket_name)

    def tearDown(self):
        delete_bucket(self._logger, self._client, self._bucket_name)

    def test_gcs(self):
        logger = self._logger

        with self.subTest(case="create_bucket"):
            create_bucket(logger, self._client, self._bucket_name)
            blob = self._client.bucket(self._bucket_name)
            self.assertTrue(blob.exists())

        with self.subTest(case="add_bucket_label"):
            add_bucket_label(logger, self._client, self._bucket_name, "test_key", "test_value")
            self.assertNotEqual(len(self._client.get_bucket(self._bucket_name).labels), 0)

        with self.subTest(case="get_bucket_label"):
            self._client.get_bucket(self._bucket_name)
            self.assertEqual(len(get_bucket_labels(logger, self._client, self._bucket_name)), 1)

    def _mock_bucket_name(self, bucket_name):
        return bucket_name.replace(".", "-")


if __name__ == "__main__":
    unittest.main()
