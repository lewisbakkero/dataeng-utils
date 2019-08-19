#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import logging

from dataeng.utils.aws import get_aws_session
from dataeng.utils.logging import get_logger
from dataeng.utils.s3 import get_s3_client, get_s3_resource


class S3UtilTest(unittest.TestCase):
    def setUp(self):
        log_name = "test-common-utils"
        self._logger = get_logger(log_name, log_level=logging.DEBUG)

    def test_get_s3_client(self):
        logger = self._logger
        aws_session = get_aws_session(logger, dict(region_name="eu-west-1"))
        s3_client = get_s3_client(logger, aws_session)
        self.assertEqual(type(s3_client).__name__, "S3")

    def test_get_s3_client_invalid_params(self):
        logger = self._logger
        self.assertRaises(AttributeError, get_s3_client, logger, None)

    def test_get_s3_resource(self):
        logger = self._logger
        aws_session = get_aws_session(logger, dict(region_name="eu-west-1"))
        s3_resource = get_s3_resource(logger, aws_session)
        self.assertEqual(type(s3_resource).__name__, "s3.ServiceResource")

    def test_get_s3_resource_invalid_params(self):
        logger = self._logger
        self.assertRaises(AttributeError, get_s3_resource, logger, None)


if __name__ == "__main__":
    unittest.main()
