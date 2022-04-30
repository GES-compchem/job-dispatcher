#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 12:40:18 2021

@author: mpalermo
"""
from multiprocessing import Process, current_process, active_children, Manager
import os
import sys
import time
from dataclasses import dataclass, field
import logging
import copy

from typing import Callable, List, Dict, Any

from packing.to_constant_volume import to_constant_volume
from packing.chunker import chunker

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
    cores: int = 1
    _dispatcher_id = None


class JobDispatcher:
    """Queue any number of different functions and execute them in parallel."""

    def __init__(
        self, jobs_list: List[Job], maxcores: int = -1, cores_per_job: int = -1
    ):

        # Set maximum total number of cores used. If user does not provide it,
        # just deduce from os settings
        self.maxcores: int

        if maxcores == -1:
            self.maxcores = len(os.sched_getaffinity(0))
            logger.debug(
                f"Total core count from os.sched_getaffinity(0): {self.maxcores}"
            )
        else:
            self.maxcores = maxcores

        self.cores_per_job: int = cores_per_job  # number of cores used per job

        if not all((isinstance(self.maxcores, int), self.maxcores > 0)):
            raise TypeError("maxcores must be a positive integer")
        # if not all((isinstance(cores_per_job, int), cores_per_job > 0)):
        #     raise TypeError("cores_per_job must be a positive integer")

        for job in jobs_list:
            self._is_it_job(job)

        self.jobs_list = jobs_list

        self.number_of_jobs: int = len(jobs_list)

        self._results_queue: Manager().Queue()

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

        if self.cores_per_job < 0 and cores is not None:
            pass
        elif self.cores_per_job > 0:
            cores = self.cores_per_job
        else:
            cores = 1

        def decorated_function(results_queue) -> None:
            """Decorate function to be returned."""
            start_time = time.time()

            os.environ["OMP_NUM_THREADS"] = str(cores)  # set maximum cores per job

            job_counter = current_process().name

            logger.debug(f"Starting job {job_name}")
            result: object = user_function(
                *args, **kwargs
            )  # run function and catch output
            results_queue.put((job_name, result))  # store the result in queue
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

    def _job_balancer(self, running_jobs_list, working_job_list):

        job_cores = sum([job.cores for job in running_jobs_list]) + 1

        if job_cores == self.maxcores:
            return []

        #if running_job_lists empty, do packing
        if running_jobs_list
        # else find first available job

        free_cores = self.maxcores - job_cores

        for index, job in enumerate(working_job_list):
            if job.cores == free_cores:
                break

        return working_job_list.pop(index)

    def run(self) -> List:
        """Run jobs in the job list."""
        # Clean up zombie processes left running from previous Runtime errors
        for process in active_children():
            if "SyncManager" in process.name:
                logger.debug("Rogue SyncManager found. Killing it.")
                process.terminate()

        self._results_queue = Manager().Queue()

        if self.number_of_jobs == 0:
            logger.info("No jobs to process. To add a new job, use the add method.")
            sys.exit()

        logger.info(
            f"Running {self.number_of_jobs} jobs on {self.maxcores} cores,"
            f" {self.cores_per_job} cores per job"
        )

        job_counter: int = 0

        working_job_list = copy.copy(self.jobs_list)
        running_jobs_list = []

        while working_job_list:
            time.sleep(0.1)  # lets not stress the CPU...
            new_jobs = self._job_balancer(running_jobs_list, working_job_list)

            for job in new_jobs:
                # run jobs
                pass

            # 1 core is occupied by the main process
            used_cores = 1 + ((len(active_children()) - 1) * self.cores_per_job)

            # Check if adding a new job would exceed the available num of cores
            if used_cores + self.cores_per_job <= self.maxcores:
                try:
                    job = self.jobs_list.pop()  # get job from queue
                    job_counter += 1
                    logger.debug(
                        f"Adding job  #{job_counter} when {used_cores} cores were used."
                    )
                    # function, args, kwargs = job.function, job.arguments, job.keyword_arguments
                    decorated_job: Callable = self._job_completion_tracker(job)
                    worker: Process = Process(
                        name=str(job_counter),
                        target=decorated_job,
                        args=(self._results_queue,),
                    )
                    worker.start()  # start job

                except IndexError:
                    # Check if there are no more jobs to run
                    if self._results_queue.qsize() == self.number_of_jobs:
                        logger.info("--- No more jobs to execute ---")
                        break

                except Exception as error:
                    logger.critical(
                        f"Critical error encountered while trying to start job n° {job_counter}"
                    )
                    logger.critical(error, exc_info=True)

        # dump: List = []

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
