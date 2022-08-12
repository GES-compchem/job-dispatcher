#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 09:56:16 2022

@author: mpalermo
"""
from abc import ABC, abstractmethod
import threading
from multiprocessing import (
    Process,
    active_children,
    Manager,
)
from typing import Callable, List
import time
import os
import logging
import copy

from jobdispatcher.jobs import Job

logger = logging.getLogger(__name__)


class Engine(ABC):
    @abstractmethod
    def add_jobs(self):
        pass

    @abstractmethod
    def run(self):
        pass


class MultithreadEngine(Engine):
    def __init__(self, cores_per_job=-1):
        self._results = {}
        self._completed_jobs = {}
        self._jobs = []
        self._job_counter = 0
        self._running_jobs = {}
        self._cores_per_job = cores_per_job
        self._threads = []
        logger.info("Using multithreading engine")

    def add_jobs(self, jobs: list):
        self._jobs.extend(jobs)

    def _initiate(self):
        pass

    def run(self):
        while self._jobs:
            job = self._jobs.pop()
            job_id = self._job_counter
            job._dispatcher_id = job_id
            self._job_counter += 1
            decorated_job: Callable = self._job_completion_tracker(job)
            thread_name = job.name + "_" + str(job_id)
            self._running_jobs[job_id] = job
            thread = threading.Thread(name=thread_name, target=decorated_job)
            thread.start()
            self._threads.append(thread)

    def _finalize(self):
        return self._results

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

        if self._cores_per_job < 0 and cores is not None:
            pass
        elif self._cores_per_job > 0:
            cores = self._cores_per_job
        else:
            cores = 1

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
                f"Elapsed time for job #{job_id} - {job_name}: {total_time:.6f} s, "
                f"{total_time/cores:.6f} s/core on {cores} cores. "
            )

        return decorated_function

    @property
    def used_cores(self):
        # this avoids the RuntimeError: dictionary changed size during iteration
        cores = copy.copy(list(self._running_jobs.values()))

        return sum([job.cores for job in cores])

    @property
    def results(self):
        for thread in self._threads:
            thread.join()

        return self._results

    @property
    def cores_per_job(self):
        return self._cores_per_job


class MultiprocessEngine(Engine):
    def __init__(self, cores_per_job=-1):
        self._jobs = []
        self._job_counter = 0
        self._running_jobs = {}
        self._cores_per_job = cores_per_job

        for process in active_children():
            if "SyncManager" in process.name:
                logger.debug("Rogue SyncManager found. Killing it.")
                process.terminate()

        # self._results_queue: Manager().Queue() = Manager().Queue()
        # self._completed_processes: Manager().Queue() = Manager().Queue()
        self.manager = Manager()
        self._running_jobs = self.manager.dict()
        self._results = self.manager.dict()

        logger.info("Using multiprocessing engine")

    def add_jobs(self, jobs: list):
        self._jobs.extend(jobs)

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

        if self._cores_per_job < 0 and cores is not None:
            pass
        elif self._cores_per_job > 0:
            cores = self._cores_per_job
        else:
            cores = 1

        def decorated_function(results, running_jobs) -> None:
            """Decorate function to be returned."""
            start_time = time.time()

            os.environ["OMP_NUM_THREADS"] = str(cores)  # set maximum cores per job

            logger.debug(f"Starting job {job_name}")
            result: object = user_function(
                *args, **kwargs
            )  # run function and catch output
            results[job_name] = result
            running_jobs.pop(job_id)

            total_time = time.time() - start_time
            logger.info(
                f"Elapsed time for job #{job_id} - {job_name}: {total_time:.6f} s, "
                f"{total_time/cores:.6f} s/core on {cores} cores. "
            )

        return decorated_function

    def run(self) -> List:
        while self._jobs:
            job = self._jobs.pop()
            job_id = self._job_counter
            job._dispatcher_id = job_id
            self._job_counter += 1
            decorated_job: Callable = self._job_completion_tracker(job)
            process_name = job.name + "_" + str(job_id)
            self._running_jobs[job_id] = job

            worker: Process = Process(
                name=process_name,
                target=decorated_job,
                args=(self._results, self._running_jobs,),
            )
            worker.start()  # start job

    @property
    def results(self):
        for child in active_children():
            if "SyncManager" in child.name:
                continue
            child.join()

        return dict(self._results)

    @property
    def used_cores(self):
        return sum([job.cores for job in self._running_jobs.values()])

    @property
    def cores_per_job(self):
        return self._cores_per_job
