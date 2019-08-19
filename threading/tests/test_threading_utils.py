#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import queue
import threading
import unittest
from time import sleep

from dataeng.utils.threading import is_thread_running


class ThreadingUtilTest(unittest.TestCase):
    def test_is_thread_running(self):
        def _test_function(q):
            q.get()
            sleep(5)
            q.task_done()

        q = queue.Queue()
        thread_name = "test_thread_name"
        t = threading.Thread(target=_test_function, args=(q,), name=thread_name)
        t.setDaemon(True)
        t.start()
        q.put("test_is_thread_running")
        self.assertTrue(is_thread_running(thread_name))
        q.join()
        self.assertFalse(is_thread_running(thread_name))


if __name__ == "__main__":
    unittest.main()
