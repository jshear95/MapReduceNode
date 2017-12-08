MapReduceNode.exe establishes a connection to a master server (defined in config) and pings it every 0.5 seconds for a map or reduce job. Upon recieving a job, this program executes it, using CPU and (if defined in config) GPU resources.

GPU usage is dependent on both being set in the config and having sufficent CUDA drivers installed for at least one CUDA capable device.

If recieving no jobs, this program is likely not broken, the server simply has no map or reduce jobs to run at the current time.

Map jobs consist of running each number in the job through a pseudoprimality test. This determines probabilistically weather or not the number is prime.

Reduce jobs consist of running each number proven to be pseudoprime through a trial by division to test for real primality.

This program was initially created for an RIT SWEN 440 resarch paper comparing traditional CPU mapreduce architectures to GPU accelerated programs (including map reduce programs). If you are reading this more than four weeks after creation date, it may no longer do this, and might have been retrofited for similar or entirely different purposes.

Author: Joshua Shear
11/17/2017
No rights reserved