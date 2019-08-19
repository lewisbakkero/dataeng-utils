#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from dataeng.utils.naming import get_ingestion_bucket_name, get_modeling_bucket_name, get_dags_bucket_name, \
    get_dataflow_bucket_name, get_dags_location, get_prefix, get_staging_dataset_id, get_test_bucket_name, \
    get_success_file_location


class NamingUtilTest(unittest.TestCase):
    def test_get_ingestion_bucket_name(self):
        result = get_ingestion_bucket_name("test", "test", "test")
        self.assertEqual("0071877d20.dataeng.com", result)

    def test_get_ingestion_bucket_name_with_caps(self):
        result = get_ingestion_bucket_name("test", "TEST", "test")
        self.assertEqual("0071877d20.dataeng.com", result)

    def test_get_modeling_bucket_name(self):
        result = get_modeling_bucket_name("test", "test", "test", "test_url")
        self.assertEqual("759ed6f3d5.dataeng.com", result)

    def test_get_dataflow_bucket_name(self):
        result = get_dataflow_bucket_name("test")
        self.assertEqual("dataflow-a94a8fe5cc.dataeng.com", result)

    def test_get_test_bucket_name(self):
        result = get_test_bucket_name("test")
        self.assertEqual("test-a94a8fe5cc.dataeng.com", result)

    def test_get_dags_bucket_name(self):
        result = get_dags_bucket_name("test")
        self.assertEqual("composer-a94a8fe5cc.dataeng.com", result)

    def test_get_prefix_without_location(self):
        result = get_prefix("gfk", "matched_report", "csv", year="2018", month="01", day="31")
        self.assertEqual("gfk/matched_report/_/2018/01/31/_/_/_/csv/", result)

    def test_get_prefix_with_location(self):
        result = get_prefix("gfk", "global_price_parity", "csv", "US", "2018", "01", "31")
        self.assertEqual("gfk/global_price_parity/US/2018/01/31/_/_/_/csv/", result)

    def test_get_prefix_with_hour(self):
        result = get_prefix("gfk", "global_price_parity", "csv", year="2018", month="01", day="31", hour="23")
        self.assertEqual("gfk/global_price_parity/_/2018/01/31/23/_/_/csv/", result)

    def test_get_prefix_with_second(self):
        result = get_prefix("gfk", "global_price_parity", "parquet", "US", year="2018", month="01", day="31", hour="23",
                            minute="59", second="59")
        self.assertEqual("gfk/global_price_parity/US/2018/01/31/23/59/59/parquet/", result)

    def test_get_dags_location(self):
        result = get_dags_location("composer-1850.dataeng.com")
        self.assertEqual("gs://composer-1850.dataeng.com/dags", result)

    def test_get_staging_dataset_id(self):
        data_source = "test-data-source"
        data_source_type = "(test-data-source-type)"
        expected_staging_bucket_name = "staging_test_data_source_test_data_source_type"
        staging_bucket_name = get_staging_dataset_id(data_source, data_source_type)
        self.assertEqual(len(staging_bucket_name), len(expected_staging_bucket_name) + 37)  # including random string
        self.assertTrue(staging_bucket_name.startswith(expected_staging_bucket_name))

    def test_get_success_file_location(self):
        data_source = "test-data-source"
        data_source_type = "test-data-source-type"
        expected_success_file_location = "test-data-source/test-data-source-type/_SUCCESS"
        success_file_location = get_success_file_location(data_source, data_source_type)
        self.assertEqual(success_file_location, expected_success_file_location)


if __name__ == "__main__":
    unittest.main()
