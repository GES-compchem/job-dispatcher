#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 11:02:24 2022

@author: mpalermo
"""
from dataclasses import dataclass, field
from typing import Callable, List, Any, Dict

@dataclass
class Job:
    """Represent a job as a function and its arguments.

    Parameters
    ----------
    function : Callable
        A function that will be called by JobDispatcher.
    arguments : List[Any]
        A list containing the function positional arguments.
    keyword_arguments : Dict[str, Any]
        A ditionary contained the function keyword arguments,
    """

    name: str
    function: Callable
    arguments: List[Any] = field(default_factory=list)
    keyword_arguments: Dict[str, Any] = field(default_factory=dict)
    cores: int
    _cores: int = field(init=False, repr=False, default=1)
    _dispatcher_id = None
    _cores_override = False
    _dispatcher_cores = None

    @property
    def cores(self):
        """ Number of cores used by the job.

        Returns
        -------
        int
            Number of cores.

        """
        if self._cores_override is True:
            return self._dispatcher_cores
        return self._cores

    @cores.setter
    def cores(self, value):
        if type(value) is property:
            # initial value not specified, use default
            value = Job._cores
        self._cores = value