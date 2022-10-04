# Job scheduling
Jobs are currently scheduled only based on the number of cores requested by the user when crating a Job instance, and the number
of available cores at the time of execution.

Job scheduling is currently implemented using a bin packing algorithm. For this reason, the order of execution is not guaranteed
to match with the one of Jobs list provided by the user.

