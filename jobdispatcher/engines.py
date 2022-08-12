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
import copy

from jobdispatcher.jobs import Job

logger = logging.getLogger(__name__)


class Engine(ABC):
    
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
    
class MultithreadedEngine(Engine):
    def __init__(self, cores_per_job = -1):
        self._results = {}
        self._completed_jobs = {}
        self._jobs = []
        self._job_counter = 0
        self._running_jobs = {}
        self._cores_per_job = cores_per_job
    
    def add_jobs(self, jobs: list):
        self._jobs.extend(jobs)
        
    
    def initiate(self):
        pass
    
    def run(self):
        
        threads = []
        
        while self._jobs:
            job = self._jobs.pop()
            job_id = self._job_counter
            job._dispatcher_id = job_id
            self._job_counter += 1
            decorated_job: Callable = self._job_completion_tracker(job)
            thread_name = job.name+"_"+ str(job_id)
            self._running_jobs[job_id] = job # order here might be important. this should be before thread.start()
            thread = threading.Thread(name = thread_name, target=decorated_job)
            threads.append(thread)
            thread.start()
            
        # for thread in threads:
        #     thread.join()
            
    
    def finalize(self):
        return self._results
    
    def used_cores(self):
        # this avoid the RuntimeError: dictionary changed size during iteration
        cores = copy.copy(list(self._running_jobs.values()))
        
        return sum([job.cores for job in cores])
    
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


        # DEV: I suspect there's some redundancy here, but it works for now... 
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
                f"Elapsed time for job #{job_id} - {job_name}: {total_time} s, "
                f"{total_time/cores} s/core on {cores} cores. "
            )

        return decorated_function
    
    
    
    
    
    
#%% From old multiprocessing version, to be reimplemented    

    # def _update_running_jobs_list(self, candidate_jobs_list):
    #     """Also returns completed jobs"""
    #     completed_ids = []
    #     completed_jobs = []
    #     elements = len(candidate_jobs_list)

    #     while True:
    #         try:
    #             ids = self._completed_processes.get_nowait()
    #             completed_ids.append(ids)
    #         except Empty:
    #             break

    #     candidates_id_toremove = []

    #     for i in range(elements):
    #         if candidate_jobs_list[i]._dispatcher_id in completed_ids:
    #             candidates_id_toremove.append(i)

    #     for i in sorted(candidates_id_toremove, reverse=True):
    #         job = candidate_jobs_list.pop(i)
    #         completed_jobs.append(job)

    #     return completed_jobs

    # def run2(self) -> List:
    #     """Run jobs in the job list."""
    #     logger.debug("-----Starting JobDispatcher logging at DEBUG level------")

    #     # Clean up zombie processes left running from previous Runtime errors
    #     for process in active_children():
    #         if "SyncManager" in process.name:
    #             logger.debug("Rogue SyncManager found. Killing it.")
    #             process.terminate()

    #     # self._results_queue = Manager().Queue()
    #     # self._completed_processes = Manager().Queue()

    #     self._results_queue = Manager().Queue()
    #     self._completed_processes = Manager().Queue()

    #     if self.number_of_jobs == 0:
    #         logger.info("No jobs to process. To add a new job, use the add method.")
    #         sys.exit()

    #     logger.info(f"Running {self.number_of_jobs} jobs")
    #     logger.info(f"Requested cores: {self.maxcores} cores")

    #     job_counter: int = 0

    #     candidate_jobs_list = copy.copy(self.jobs_list)
    #     running_jobs_list = []
    #     completed_jobs_list = []

    #     timer = time.perf_counter()

    #     while candidate_jobs_list:
    #         completed_jobs_list += self._update_running_jobs_list(running_jobs_list)
            
    #         unavailable_cores = sum([job.cores for job in running_jobs_list]) + 1

    #         new_jobs = self._job_balancer.run(unavailable_cores, candidate_jobs_list)

    #         time.sleep(0.002)  # lets not stress the CPU, Queue.get is slow anyway

    #         for job in new_jobs:
    #             # run jobs
    #             job_counter += 1
    #             job._dispatcher_id = job_counter
    #             decorated_job: Callable = self._job_completion_tracker(job)
    #             worker: Process = Process(
    #                 name=str(job_counter),
    #                 target=decorated_job,
    #                 args=(self._results_queue, self._completed_processes),
    #             )
    #             running_jobs_list.append(job)
    #             worker.start()  # start job

    #     while len(completed_jobs_list) != len(self.jobs_list):
    #         completed_jobs_list += self._update_running_jobs_list(running_jobs_list)

    #     elapsed = time.perf_counter() - timer
    #     logger.info(
    #         f"Jobs completed in {elapsed} s, average of {elapsed/self.number_of_jobs} s/job."
    #     )

    #     dump: Dict = {}

    #     logger.debug("Retrieving results from results queue")
    #     for _ in range(self.number_of_jobs):
    #         name, result = self._results_queue.get()
    #         dump[name] = result

    #     # For some reason, after jobs have finished, the SyncManager object remains
    #     # active, and this is a problem in an interactive python shell because
    #     # the active children count is then flawed. This makes sure the Syncmanager
    #     # object is terminated.

    #     for process in active_children():
    #         if "SyncManager" in process.name:
    #             process.terminate()

    #     return dump