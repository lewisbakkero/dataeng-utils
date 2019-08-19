#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import logging

from dataeng.utils.aws import get_aws_session, get_aws_keys_from_profile
from dataeng.utils.logging import get_logger


class AWSCommonUtilTest(unittest.TestCase):
    def setUp(self):
        log_name = "test-common-utils"
        self._logger = get_logger(log_name, log_level=logging.DEBUG)

    def test_get_aws_session(self):
        logger = self._logger
        aws_session = get_aws_session(logger, dict(region_name="eu-west-1"))
        self.assertEqual(type(aws_session).__name__, "Session")

    def test_get_aws_session_invalid_params(self):
        logger = self._logger
        aws_session = get_aws_session(logger, dict(region_name="eu-west-1", invalid_param="invalid_param"))
        self.assertIsNone(aws_session)

    def test_get_aws_session_invalid_profile(self):
        logger = self._logger
        aws_session = get_aws_session(logger, dict(profile_name="invalid_profile", region_name="eu-west-1"))
        self.assertIsNone(aws_session)

    def test_get_aws_keys_from_profile(self):
        logger = self._logger
        aws_access_key_id = "test_access_key"
        aws_secret_access_key = "test_secret_key"
        aws_session = get_aws_session(logger, dict(aws_access_key_id=aws_access_key_id,
                                                   aws_secret_access_key=aws_secret_access_key))
        access_key, secret_key = get_aws_keys_from_profile(logger, aws_session)
        self.assertEqual(access_key, aws_access_key_id)
        self.assertEqual(secret_key, aws_secret_access_key)

    def test_get_aws_keys_from_profile_invalid_session(self):
        logger = self._logger
        self.assertRaises(AttributeError, get_aws_keys_from_profile, logger, None)


if __name__ == "__main__":
    unittest.main()
