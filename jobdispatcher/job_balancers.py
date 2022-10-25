import logging
from jobdispatcher.packing.to_constant_volume import to_constant_volume
from jobdispatcher.packing.chunker import chunker

logger = logging.getLogger(__name__)


class JobBalancer:
    """
    Determines which jobs should be provided next to the engine.
    """

    def __init__(self, maxcores):
        """
        Init function.

        Parameters
        ----------
        maxcores : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """

        self.maxcores = maxcores
        self._last_state = None

    def run(self, unavailable_cores, candidate_jobs_list):
        """
        Job balancer returns a list of jobs to be run from the engine.

        Parameters
        ----------
        running_jobs_list : TYPE
            DESCRIPTION.
        candidate_jobs_list : TYPE
            DESCRIPTION.

        Raises
        ------
        ValueError
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """

        # number of occupied cores
        # unavailable_cores = sum([job.cores for job in running_jobs_list]) + 1

        jobs_to_be_run = []

        if unavailable_cores == self.maxcores:
            return jobs_to_be_run

        # else find available jobs

        free_cores = self.maxcores - unavailable_cores - 1  # -1 to keep master core free

        if free_cores < 0:
            raise ValueError(
                "DEV: Negative number of free cores. Faulty code logic should be inspected."
            )

        # Check if number of used cores has changed since the last time _job_balancer
        # has been called
        if self._last_state is None:
            pass
        elif free_cores == self._last_state:
            return jobs_to_be_run

        logger.debug(f"----- In _job_balancer: {free_cores} CORES NEEDED -----")

        return self._balance(candidate_jobs_list, free_cores)

    def _balance(self, candidate_jobs_list, free_cores):

        jobs_to_be_run = []

        number_of_candidates = len(candidate_jobs_list)
        logging.debug("----- Rebalancing through BIN PACKING ----")

        chunk_counter = 0

        # chunker breaks the list lenght in chunks of optimal size and returns the indexes
        # so that binpacking does not have to work on huge lists

        for start, end in chunker(number_of_candidates):
            chunk_counter += 1  # not pythonic but enumerate would be unreadable
            logger.debug(f"BIN PACKING: Exploring chunk {chunk_counter}")

            if end is None:
                end = number_of_candidates
            index_cores = [
                (index, candidate_jobs_list[index].cores) for index in range(start, end)
            ]

            packs = to_constant_volume(
                index_cores,
                free_cores,
                weight_pos=1,
            )

            # Sometimes the fullest pack is not at the forefront, let's reorder
            def sorting_func(pack):
                cores = sum(job[1] for job in pack)

                if cores > free_cores:
                    cores = 0

                return cores

            # new version

            sorted_packs = sorted(packs, key=sorting_func, reverse=True)

            cores = sum(job[1] for job in sorted_packs[0])

            # if no combination of jobs sums up to free_cores or less, try with next chunk
            if cores > free_cores:
                logger.debug(
                    f"BIN PACKING: Picked pack has {cores} cores when "
                    f"{free_cores} cores are free. Retrying."
                )
                self._last_state = free_cores
                continue
            self._last_state = None

            logger.debug(
                f"BIN PACKING: Picked pack has {cores} cores when {free_cores}"
                f" cores are free. {free_cores-cores} cores unused."
            )

            # We are going to consider only the first pack of jobs, as the
            # sorting function orders from the pack with the highest number of
            # cores WITHIN free_cores, followed by packs with less cores and,
            # at the end of the list, the packs that exceeds free_cores
            indexes = [job[0] for job in sorted_packs[0]]

            jobs_to_be_run = []

            # remove selected jobs from the candidate_jobs_list and put then in the list
            # of jobs to be executed
            for index in sorted(indexes, reverse=True):
                job = candidate_jobs_list.pop(index)
                jobs_to_be_run.append(job)
                logger.debug(f'BIN PACKING: Adding job "{job.name}" ({job.cores} cores)')
            logger.debug(
                f"----- BALANCER OUTPUT: Using "
                f"{self.maxcores-free_cores+cores}/{self.maxcores} cores -----"
            )

            break
        else:
            logger.debug(
                f"----- BALANCER OUTPUT: No solution found. Retrying. "
                f"{self.maxcores-free_cores}/{self.maxcores} cores -----"
            )

        return jobs_to_be_run
