#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from dataeng.utils.data_type import represent_int


class DataTypeUtilTest(unittest.TestCase):
    def test_represent_int_true(self):
        valid_str_representations = ["0", "123"]

        for s in valid_str_representations:
            self.assertTrue(represent_int(s))

    def test_represent_int_false(self):
        invalid_str_representations = ["a", "1a", "a1"]

        for s in invalid_str_representations:
            self.assertFalse(represent_int(s))


if __name__ == "__main__":
    unittest.main()
