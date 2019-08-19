#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import unittest

from dataeng.utils.logging import get_logger


class LoggingUtilTest(unittest.TestCase):
    def test_get_logger(self):
        log_name = "test-common-utils"
        logger = get_logger(log_name)
        self.assertTrue(isinstance(logger, logging.Logger))
        self.assertEqual(log_name, logger.name)


if __name__ == "__main__":
    unittest.main()
