#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading


def is_thread_running(thread_name):
    """
    Determines if there's an active thread for a given name.

    :param thread_name: thread name

    :type thread_name: str

    :returns: True if the thread for a given name is running, False otherwise
    :rtype: bool
    """
    thread_list = threading.enumerate()

    for t in thread_list:
        if t.name == thread_name:
            return True

    return False
