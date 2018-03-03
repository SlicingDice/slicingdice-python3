#!/usr/bin/env python
# -*- coding: utf-8 -*-


def is_dict_empty(dictionary):
    """Validate if a dictionary is empty or no. Returns a boolean value.

    Keyword arguments:
    dictionary(dict) -- A dictionary to analyze
    """
    return dictionary is None or len(dictionary) == 0


def is_str_empty(string):
    """Validate if a string is empty or no. Returns a boolean value.

    Keyword arguments:
    string(str) -- A string to analyze
    """
    return string.isspace() or not string
