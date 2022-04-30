#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 16:13:16 2022

@author: mpalermo
"""


def chunker(list_lenght, chunk_size=100, limiter=2):
    upper_bound = chunk_size * limiter + 1
    if list_lenght <= upper_bound:
        splitter = list_lenght

    results = {}

    for tentative_chunk in range(chunk_size, upper_bound):
        remainder = list_lenght % tentative_chunk
        if remainder == 0:
            splitter = tentative_chunk
            break
        else:
            results[remainder] = tentative_chunk

    else:
        splitter = results[max(list(results.keys()))]

    # print(splitter, list_lenght % splitter, list_lenght // splitter)

    starting_slicer = 0

    while starting_slicer < list_lenght:
        end_slicer = starting_slicer + splitter
        if end_slicer > list_lenght:
            end_slicer = None
        # print(starting_slicer, end_slicer)
        # splitted_list.append(a_list[starting_slicer:end_slicer])
        last_slicer = starting_slicer
        starting_slicer += splitter

        yield last_slicer, end_slicer
