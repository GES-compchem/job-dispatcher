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
    def cores(self):  # pylint: disable=E0102
        """Number of cores used by the job.

        Returns
        -------
        int
            Number of cores.

        """
        if self._cores_override is True:
            return self._dispatcher_cores
        return self._cores

    @cores.setter
    def cores(self, cores):
        if isinstance(cores, property):
            # initial value not specified, use default
            cores = Job._cores
        self._cores = cores

    @property
    def dispatcher_cores(self):
        return self._dispatcher_cores

    @dispatcher_cores.setter
    def dispatcher_cores(self, value):
        self._dispatcher_cores = value

    @property
    def cores_override(self):
        return self._cores_override

    @cores_override.setter
    def cores_override(self, override=False):
        self._cores_override = override
