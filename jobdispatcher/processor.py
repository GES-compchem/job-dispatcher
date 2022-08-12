#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 12:40:18 2021

@author: mpalermo
"""

import os
import sys
import logging
import copy
from typing import List, Callable

from jobdispatcher.job_balancers import JobBalancer
from jobdispatcher.engines import MultithreadedEngine
from jobdispatcher.jobs import Job

logger = logging.getLogger(__name__)


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

        self.jobs_list = jobs_list

        self.number_of_jobs: int = len(jobs_list)

        self._job_balancer = JobBalancer(self.maxcores)
        self._engine = MultithreadedEngine(cores_per_job=cores_per_job)
        self.results = {}
        
        
        for job in jobs_list:
            self._is_it_job(job)

    def _is_it_job(self, job: Callable) -> None:
        """Check if the input job is a function."""
        if not isinstance(job, Job):
            raise TypeError(
                f"The provided job \x1b[1;37;34m{job}\x1b[0m is not an instance"
                " of jobdispatcher.Job.\n"
                "Please initialize a Job object as Job(function) and then pass"
                " it to JobDispatcher."
            )

        if self._engine._cores_per_job > 0:
            job._cores_override = True
            job._dispatcher_cores = self._engine._cores_per_job
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



    def run(self):
        """Run jobs in the job list."""
        logger.debug("-----Starting JobDispatcher logging at DEBUG level------")
        
        if self.number_of_jobs == 0:
            logger.info("No jobs to process. To add a new job, use the add method.")
            sys.exit()

        logger.info(f"Running {self.number_of_jobs} jobs")
        logger.info(f"Requested cores: {self.maxcores} cores")
        
        candidate_jobs_list = copy.copy(self.jobs_list)
        job_balancer = self._job_balancer
        engine = self._engine
        
        while candidate_jobs_list:
            used_cores = engine.used_cores()
            new_jobs = job_balancer.run(used_cores, candidate_jobs_list)
            engine.add_jobs(new_jobs)
            engine.run()
        
        self.results = engine.finalize()
        
        return self.results 