from typing import List, Any, Callable
from multiprocessing import cpu_count
from abc import ABC, abstractmethod
from jobdispatcher import Job, JobDispatcher


class Scheduler(ABC):
    """
    Abstract base class setting the basic functionality of a job scheduler. Given a number
    of available cores and a number of simulations to schedule, it returns, thanks to the
    `get_chunks` abstract method, the sequence of task indeces to be runned within each
    process. By overloading the `get_chunks` method, different types of scheduling can be
    implemented.

    Arguments
    ---------
    cores: int
        the number of cores available to share the workload. If the number of cores is set to
        a non-positive value or if the number of cores exceeds the number of cpus available to
        multiprocessing, the number of cores will be set to the latter value.
    """

    def __init__(self, cores: int) -> None:
        super().__init__()

        self.tasks = None
        self.__cores = cpu_count()

        if cores > 0 and cores <= self.__cores:
            self.__cores = cores

    @property
    def cores(self) -> int:
        """
        The number of cores across which the workload will be shared.

        Returns
        -------
        int
            The number of available cores.
        """
        return self.__cores

    @abstractmethod
    def get_chunks(self, tasks: int) -> List[List[int]]:
        """
        Abstract method returning the list of chunks to be execured within each process.
        Each chunck is represented by a list of positive integers encoding the identifier of
        the task that must be run.

        Arguments
        ---------
        tasks: int
            The total number of tasks to schedule.

        Raises
        ------
        ValueError
            Exception raised when an invalid value has been passed as the `tasks` argument.

        Returns
        -------
        List[List[int]]
            The list of chunks to be assigned to each process. Each chunk encodes the sequence
            of the tasks to be executed in each process.
        """
        if tasks <= 0:
            raise ValueError("Cannot compute a schedule for a non-positive `tasks` value")

        self.tasks = tasks


class Static(Scheduler):
    """
    A simple static scheduler equally dividing the number of jobs across the available cores.
    The excess number of jobs will be assigned to the last cores.

    Arguments
    ---------
    cores: int
        the number of cores available to share the workload.
    """

    def __init__(self, cores: int) -> None:
        super().__init__(cores)

    def get_chunks(self, tasks: int) -> List[int]:
        """
        The functions returns the static schedule associated with the subdivision of a given
        number (`tasks`) of iterations across all the available cores. If the number of tasks
        is not exactly divisible for the number of cores the excess workload will be added
        equally on the last few cores.

        Arguments
        ---------
        tasks: int
            The total number of tasks to schedule.

        Raises
        ------
        ValueError
            Exception raised when an invalid value has been passed as the `tasks` argument.

        Returns
        -------
        List[List[int]]
            The list of chunks to be assigned to each core. Each chunk encodes the sequence
            of the tasks to be executed in each process. A single process will be assigned to
            each core.
        """
        super().get_chunks(tasks)

        if tasks < self.cores:
            raise RuntimeError(
                "Cannot use a Static scheduler with a number of task smaller than the number of cores"
            )

        extra = self.tasks % self.cores
        load = int((self.tasks - extra) / self.cores)

        chunks = []
        for core in range(self.cores):
            tasks = load if extra < (self.cores - core) else load + 1
            last = 0 if chunks == [] else chunks[-1][-1] + 1
            chunks.append([last + n for n in range(tasks)])

        return chunks


class Dynamic(Scheduler):
    """
    A simple dynamic scheduler dividing the number of jobs across the available cores in a
    set of multiple chuncks the size of which can be limited.

    Arguments
    ---------
    cores: int
        the number of cores available to share the workload.
    chunk_size: int
        the maximum size of each chunk of tasks
    """

    def __init__(self, cores: int, chunk_size: int) -> None:
        super().__init__(cores)

        if chunk_size < 1:
            raise ValueError("The `chunk_size` value cannot be smaller than one.")

        self.__size = chunk_size

    def get_chunks(self, tasks: int) -> List[List[int]]:
        """
        The functions returns the dynamic schedule associated with the subdivision of a `tasks`
        number of iterations across all the available cores

        Arguments
        ---------
        tasks: int
            The total number of tasks to schedule.

        Raises
        ------
        ValueError
            Exception raised when an invalid value has been passed as the `tasks` argument.

        Returns
        -------
        List[List[int]]
            The list of chunks to be assigned to each process. Each chunk encodes a sequence
            of the tasks of at most `chunk_size` elements to be assigned to the processes. In
            general more chunks than cores are generated and each core is dynalically assigned
            a chunk of tasks.
        """
        super().get_chunks(tasks)

        extra = self.tasks % self.__size
        nchunks = int((self.tasks - extra) / self.__size)

        chunks = []
        for chunk in range(nchunks):
            chunks.append([chunk * self.__size + n for n in range(self.__size)])

        if extra != 0:
            chunks.append([self.__size * nchunks + n for n in range(extra)])

        return chunks


class pFor:
    """
    The pFor class implements a parallel, index-based, for loop capable of executing in parallel
    independent tasks identified by a univocal index. The pFor object is constructed by specifying
    a schedule mode. By then using the built in `__call__` function the for loop can be executed.

    Arguments
    ---------
    scheduler: Scheduler
        A scheduler object derived from the `Scheduler` abstract base class

    Raises
    ------
    TypeError
        Exception raised if the scheduler object is not derived form the `Scheduler` base class
    """

    def __init__(self, scheduler: Scheduler) -> None:

        if not isinstance(scheduler, Scheduler):
            raise TypeError(
                "The `schedule` argument must be an object derived from the Schedule class"
            )

        self.__scheduler = scheduler

    def __job_runner(
        self, sequence: List[int], function: Callable, args: List[Any]
    ) -> List[Any]:
        """
        Built in function internally used to run a sequence of index based calls to the user
        provided function.

        Arguments
        ---------
        sequence: List[int]
            The list containing all the integers to be passed as arguments to the desired function.
        function: Callable
            Any function of the type `f(n, args)` where `n` represents the index that identifies
            the iteration.
        args: List[Any]
            The list of arguments to be passed to the given function.

        Returns
        -------
        List[Any]
            The list containing all the retuns associated to the provided sequence of indeces.
        """
        results = []
        for index in sequence:
            r = function(index, *args)
            results.append(r)

        return results

    def __call__(self, function, start, end, step=1, args=[]) -> List[Any]:
        """
        Function capable of running the index-based job sequence using the provided schedule.

        Arguments
        ---------
        function: Callable
            Any function of the type `f(n, args)` where `n` represents the index that identifies
            the iteration.
        start: The index from which the iteration must be started.
        end: The final index (excluded) at which the iteration must be stopped.
        step: The step to use in the exploration of the index range.
        args: List[Any]
            The list of arguments to be passed to the given function.

        Returns
        -------
        List[Any]
            The list containing all the retuns associated to the sequence of indeces in the
            provided range.
        """

        if step == 0:
            raise ValueError("The value of `step` cannot be zero")
        elif start < end and step < 0:
            raise RuntimeError(
                "Cannot use a negative step if `start` is smaller than `end`"
            )
        elif start > end and step > 0:
            raise RuntimeError(
                "Cannot use a positive step if `start` is greater than `end`"
            )

        indeces = [n for n in range(start, end, step)]
        workloads = self.__scheduler.get_chunks(len(indeces))

        jobs = []
        for order, work in enumerate(workloads):
            sequence = [indeces[w] for w in work]
            job = Job(f"{order}", self.__job_runner, arguments=[sequence, function, args])
            jobs.append(job)

        dispatcher = JobDispatcher(jobs, maxcores=self.__scheduler.cores)
        buffer = dispatcher.run()

        results = []
        for order, _ in enumerate(workloads):
            for entry in buffer[f"{order}"]:
                results.append(entry)

        return results
