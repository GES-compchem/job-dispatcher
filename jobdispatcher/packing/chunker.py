#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 16:13:16 2022

@author: mpalermo
"""


def chunker(list_lenght, chunk_size=100, limiter=2):
    """
    Generator function that returns.

    Parameters
    ----------
    list_lenght : TYPE
        DESCRIPTION.
    chunk_size : TYPE, optional
        DESCRIPTION. The default is 100.
    limiter : TYPE, optional
        DESCRIPTION. The default is 2.

    Yields
    ------
    last_slicer : TYPE
        DESCRIPTION.
    end_slicer : TYPE
        DESCRIPTION.

    """
    upper_bound = chunk_size * limiter + 1
    if list_lenght <= upper_bound:
        splitter = list_lenght

    results = {}

    # find if any number between chunk size and upper bound is a divisor of the input number
    # remainders for any tentative chunk_size is saved in results dictionary
    for tentative_chunk in range(chunk_size, upper_bound):
        remainder = list_lenght % tentative_chunk
        if remainder == 0:
            splitter = tentative_chunk
            break

        results[remainder] = tentative_chunk

    else:
        # if no divisor is found, take the tentative chunk size with the biggest reminder!
        splitter = results[max(list(results.keys()))]

    # print(splitter, list_lenght % splitter, list_lenght // splitter)

    starting_slicer = 0

    # generator that yields starting and final indexes of the chunks
    while starting_slicer < list_lenght:
        end_slicer = starting_slicer + splitter
        if end_slicer > list_lenght:
            end_slicer = None
        # print(starting_slicer, end_slicer)
        last_slicer = starting_slicer
        starting_slicer += splitter

        yield last_slicer, end_slicer
