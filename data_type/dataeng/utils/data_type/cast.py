#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def represent_int(value):
    """
    Checks if a string represents an int.

    :param value: string to be checked

    :type value: str

    :returns: True if the string represents an int, False otherwise
    :rtype: bool
    """
    try:
        int(value)
        return True
    except ValueError:
        return False
