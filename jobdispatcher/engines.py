#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 09:56:16 2022

@author: mpalermo
"""
from abc import ABC, abstractmethod
import threading
from typing import Callable
import time
import os
import logging

from jobdispatcher.processor import Job

logger = logging.getLogger(__name__)


def Engine(ABC):
    @abstractmethod
    def initiate(self):
        pass

    @abstractmethod
    def add_jobs(self):
        pass
    
    @abstractmethod
    def run(self):
        pass    

    @abstractmethod   
    def finalize(self):
        pass
    
    @abstractmethod   
    def used_cores(self):
        pass
    
def MultithreadedEngine(Engine):
    def __init__(self):
        self._results = {}
        self._completed_jobs = {}
        self._jobs = []
        self._job_counter = 0
        self._running_jobs = {}
    
    def add_jobs(self, jobs: list):
        self.jobs.append(jobs)
        
    
    def initiate(self):
        pass
    
    def run(self):
        while self._jobs:
            job = self._jobs.pop()
            job_id = self._job_counter
            self._job_counter += 1
            decorated_job: Callable = self._job_completion_tracker(job)
            thread_name = job.name+"_"+ str(self._job_counter)
            thread = threading.Thread(name = thread_name, target=decorated_job)
            self._running_jobs[job_id] = job # order here might be important. this should be before thread.start()
            thread.start()
            
    
    def finalize(self):
        pass
    
    def used_cores(self):
        return sum([job.cores for job in self._running_jobs.values()])
    
    def _job_completion_tracker(self, job: Job) -> Callable:
        """
        Take user function and decorate it by setting the number of cores
        available to the jobs, gather the results in a common queue and update
        the number of available cores once the functions terminates.

        Parameters
        ----------
        user_function : method
            User function to be run

        Returns
        -------
        decorated_function : method
            Function decorated for the inner working of the program.
            .

        """
        job_name = job.name
        user_function = job.function
        args = job.arguments
        kwargs = job.keyword_arguments
        cores = job.cores
        job_id = job._dispatcher_id

        # if self.cores_per_job < 0 and cores is not None:
        #     pass
        # elif self.cores_per_job > 0:
        #     cores = self.cores_per_job
        # else:
        #     cores = 1

        def decorated_function() -> None:
            """Decorate function to be returned."""
            start_time = time.time()

            os.environ["OMP_NUM_THREADS"] = str(cores)  # set maximum cores per job

            logger.debug(f"Starting job {job_name}")
            result: object = user_function(
                *args, **kwargs
            )  # run function and catch output
            self._results[job_name] = result
            finished_job = self._running_jobs.pop(job_id)
            self._completed_jobs[job_id] = finished_job
            
            total_time = time.time() - start_time
            logger.info(
                f"Elapsed time for job #{job_id} - {job_name}: {total_time} s, "
                f"{total_time/cores} s/core on {cores} cores. "
            )

        return decorated_function