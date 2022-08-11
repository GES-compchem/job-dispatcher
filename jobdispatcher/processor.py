#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 12:40:18 2021

@author: mpalermo
"""
from multiprocessing import (
    Process,
    active_children,
    Manager,
)
import os
import sys
import time
from dataclasses import dataclass, field
from queue import Empty

import logging
import copy

from typing import Callable, List, Dict, Any

# from jobdispatcher.packing.to_constant_volume import to_constant_volume
# from jobdispatcher.packing.chunker import chunker

from jobdispatcher.job_balancers import JobBalancer

logger = logging.getLogger(__name__)


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


class JobDispatcher:
    """Queue any number of different functions and execute them in parallel."""

    def __init__(
        self, jobs_list: List[Job], maxcores: int = -1, cores_per_job: int = -1
    ):
        logger.debug("-----Starting JobDispatcher object initialization.-----")
        logger.debug(f"Object: ({self})")
        # Set maximum total number of cores used. If user does not provide it,
        # just deduce from os settings
        self.maxcores: int

        if not isinstance(maxcores, int):
            raise TypeError("maxcores must be an integer number")
        if not isinstance(cores_per_job, int):
            raise TypeError("cores_per_job must be an integer number")

        available_cores = len(os.sched_getaffinity(0))

        if maxcores <= 0:
            self.maxcores = available_cores
            logger.debug(
                f"Total core count from os.sched_getaffinity(0): {available_cores}"
            )
        elif maxcores > available_cores:
            self.maxcores = maxcores
            logger.warning(
                f"Requested maximum number of cores ({maxcores}) exceeds system"
                f" resources ({available_cores}). I hope you know what you're doing."
            )
        else:
            self.maxcores = maxcores

        self.cores_per_job: int = cores_per_job  # number of cores used per job

        for job in jobs_list:
            self._is_it_job(job)

        self.jobs_list = jobs_list

        self.number_of_jobs: int = len(jobs_list)

        self._results_queue: Manager().Queue()

        self._completed_processes: Manager().Queue()

        self._job_balancer = JobBalancer(self.maxcores)

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
        job_counter = job._dispatcher_id

        if self.cores_per_job < 0 and cores is not None:
            pass
        elif self.cores_per_job > 0:
            cores = self.cores_per_job
        else:
            cores = 1

        def decorated_function(results_queue, completed_queue) -> None:
            """Decorate function to be returned."""
            start_time = time.time()

            os.environ["OMP_NUM_THREADS"] = str(cores)  # set maximum cores per job

            logger.debug(f"Starting job {job_name}")
            result: object = user_function(
                *args, **kwargs
            )  # run function and catch output
            results_queue.put_nowait((job_name, result))  # store the result in queue
            completed_queue.put(job_counter)
            total_time = time.time() - start_time
            logger.info(
                f"Elapsed time for job #{job_counter} - {job_name}: {total_time} s, "
                f"{total_time/cores} s/core on {cores} cores. "
            )

        return decorated_function

    def _is_it_job(self, job: Callable) -> None:
        """Check if the input job is a function."""
        if not isinstance(job, Job):
            raise TypeError(
                f"The provided job \x1b[1;37;34m{job}\x1b[0m is not an instance"
                " of jobdispatcher.Job.\n"
                "Please initialize a Job object as Job(function) and then pass"
                " it to JobDispatcher."
            )

        if self.cores_per_job > 0:
            job._cores_override = True
            job._dispatcher_cores = self.cores_per_job
        else:
            job._cores_override = False

        if job.cores >= self.maxcores:
            raise ValueError(
                f"Job {job.name} ({job.cores} cores) exceedes the assigned"
                f" resources ({self.maxcores} cores)."
            )

    def add(self, job: Job):
        """Add a new job to the job list.

        Parameters
        ----------
        job: Job
            A Job type instance.
        Returns
        -------
        None

        """
        self._is_it_job(job)
        self.jobs_list.append(job)
        self.number_of_jobs += 1

    def _update_running_jobs_list(self, candidate_jobs_list):
        """Also returns completed jobs"""
        completed_ids = []
        completed_jobs = []
        elements = len(candidate_jobs_list)

        while True:
            try:
                ids = self._completed_processes.get_nowait()
                completed_ids.append(ids)
            except Empty:
                break

        candidates_id_toremove = []

        for i in range(elements):
            if candidate_jobs_list[i]._dispatcher_id in completed_ids:
                candidates_id_toremove.append(i)

        for i in sorted(candidates_id_toremove, reverse=True):
            job = candidate_jobs_list.pop(i)
            completed_jobs.append(job)

        return completed_jobs

    def run(self) -> List:
        """Run jobs in the job list."""
        logger.debug("-----Starting JobDispatcher logging at DEBUG level------")

        # Clean up zombie processes left running from previous Runtime errors
        for process in active_children():
            if "SyncManager" in process.name:
                logger.debug("Rogue SyncManager found. Killing it.")
                process.terminate()

        # self._results_queue = Manager().Queue()
        # self._completed_processes = Manager().Queue()

        self._results_queue = Manager().Queue()
        self._completed_processes = Manager().Queue()

        if self.number_of_jobs == 0:
            logger.info("No jobs to process. To add a new job, use the add method.")
            sys.exit()

        logger.info(f"Running {self.number_of_jobs} jobs")
        logger.info(f"Requested cores: {self.maxcores} cores")

        job_counter: int = 0

        candidate_jobs_list = copy.copy(self.jobs_list)
        running_jobs_list = []
        completed_jobs_list = []

        timer = time.perf_counter()

        while candidate_jobs_list:
            completed_jobs_list += self._update_running_jobs_list(running_jobs_list)
            
            unavailable_cores = sum([job.cores for job in running_jobs_list]) + 1

            new_jobs = self._job_balancer.run(unavailable_cores, candidate_jobs_list)

            time.sleep(0.002)  # lets not stress the CPU, Queue.get is slow anyway

            for job in new_jobs:
                # run jobs
                job_counter += 1
                job._dispatcher_id = job_counter
                decorated_job: Callable = self._job_completion_tracker(job)
                worker: Process = Process(
                    name=str(job_counter),
                    target=decorated_job,
                    args=(self._results_queue, self._completed_processes),
                )
                running_jobs_list.append(job)
                worker.start()  # start job

        while len(completed_jobs_list) != len(self.jobs_list):
            completed_jobs_list += self._update_running_jobs_list(running_jobs_list)

        elapsed = time.perf_counter() - timer
        logger.info(
            f"Jobs completed in {elapsed} s, average of {elapsed/self.number_of_jobs} s/job."
        )

        dump: Dict = {}

        logger.debug("Retrieving results from results queue")
        for _ in range(self.number_of_jobs):
            name, result = self._results_queue.get()
            dump[name] = result

        # For some reason, after jobs have finished, the SyncManager object remains
        # active, and this is a problem in an interactive python shell because
        # the active children count is then flawed. This makes sure the Syncmanager
        # object is terminated.

        for process in active_children():
            if "SyncManager" in process.name:
                process.terminate()

        return dump
