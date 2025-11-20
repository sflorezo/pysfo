#%%

#---- auxiliary functions

def deep_size(obj, seen=None):

    import sys

    """Recursively estimate memory size of nested containers"""

    if seen is None:
        seen = set()
    
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        size += sum(deep_size(k, seen) + deep_size(v, seen) for k, v in obj.items())
    elif isinstance(obj, (list, tuple, set)):
        size += sum(deep_size(i, seen) for i in obj)
    return size

#---- main function

def test_batchjob(jobfn, 
                  batch_njobs, 
                  total_batches, 
                  testbatches, 
                  safethresh_GB, 
                  **kwargs):
    
    """
    Test the batch job function for memory usage and time per batch.

    Parameters:
    - jobfn: The function to be tested.
    - batch_njobs: Number of jobs to run in parallel. If -1, uses all available CPUs.
    - total_batches: Total number of batches in the job.
    - testbatches: List of batches to test.
    - safethresh_GB: Safety threshold for available memory in GB.
    - **kwargs: Additional arguments to pass to the job function.

    Returns:
    - safe_job: Boolean indicating if the job is safe to run based on memory and time assessments.
    """

    import time
    import joblib
    import psutil

    print(f"Number of batches in testing: {len(testbatches)}")
    print(f"-----------------------------\n")
    memory_mb = 0
    start = time.time() 
    for batch in testbatches:
        sample_batch = jobfn(batch, **kwargs)
        memory_bytes = deep_size(sample_batch)
        memory_mb += memory_bytes / 1e6
    end = time.time()  # End timing
    elapsed_per_batch = (end - start) / len(testbatches)
    memory_mb = memory_mb / len(testbatches)

    # number of workers planned to be used

    est_njobs = joblib.cpu_count() if batch_njobs == -1 else batch_njobs

    # Estimated total RAM needed
    estimated_total_mb = memory_mb * est_njobs
    available_gb = psutil.virtual_memory().available / 1e9

    # raise memory safety errors

    if available_gb < safethresh_GB:
        raise MemoryError(f"Aborting: only {available_gb:.1f} GB free, but {safethresh_GB} GB required.")

    if estimated_total_mb / 1e3 > (available_gb - safethresh_GB):
        raise MemoryError(
            f"Aborting: estimated total memory use ({estimated_total_mb / 1e3:.1f} GB) "
            f"would drop below safe free threshold of {safethresh_GB} GB."
        )
    
    # pass memory tests
    print(f"Memory tests passed:")
    print(f"---------------------")
    print(f"- Estimated total memory use ({estimated_total_mb / 1e3:.1f} GB)")
    print(f"- (Available GB - Safety Free GB): ({available_gb - safethresh_GB:.1f} GB)\n")
    safe_job = True

    # raise warning if elapsed_per_batch not ideal

    print("Assessment of time per batch")
    print(f"----------------------------")
    if elapsed_per_batch < 0.1:
        print(f"- Too short ({elapsed_per_batch:.2f} seg/batch) — high scheduling overhead likely. Consider increasing batch size.")
    elif elapsed_per_batch < 0.3:
        print(f"- Borderline short ({elapsed_per_batch:.3f}). You can likely increase batch size for better efficiency.")
    elif elapsed_per_batch <= 1.5:
        print(f"- Ideal  ({elapsed_per_batch:.3f} seg/batch) — batch size is in the efficient range.")
    elif elapsed_per_batch <= 5.0:
        print(f"- Acceptable  ({elapsed_per_batch:.3f} seg/batch) — may be fine if memory is well utilized. Watch for uneven load.")
    else:
        print(f"- Too long  ({elapsed_per_batch:.3f} seg/batch) — reduce batch size to better balance load across workers.")

    # estimated total time of batch job
    print(f"- Lower bound for total time of batch job:")
    lower_bound_time = round((total_batches * elapsed_per_batch) / (est_njobs * 60), 2)
    print(f"  - {lower_bound_time:.2f} mins [ (total_batches * time_per_batch) / n_jobs ]\n")
    
    return safe_job

def start_monitor_memory(mem_threshold=0.95, check_interval=1.0):
    """
    Runs a batch of parallel jobs safely with a memory monitor that kills all jobs if memory exceeds threshold.

    Parameters:
    - func: the function to be called (will be wrapped in delayed)
    - args_list: list of argument tuples for each call to func
    - n_jobs: number of parallel workers
    - verbose: verbosity for joblib
    - mem_threshold: system memory usage threshold (0.0–1.0) at which to kill the job
    - check_interval: how often to check memory (in seconds)
    """

    import psutil
    import os
    import signal
    import time
    import threading

    def _monitor_memory():
        parent = psutil.Process(os.getpid())
        termination_signal = signal.SIGTERM if os.name == "nt" else signal.SIGKILL

        while True:
            used = psutil.virtual_memory().percent
            if used >= mem_threshold * 100:
                print(f"[MEMORY MONITOR] Memory usage {used:.2f}% exceeded {mem_threshold*100:.0f}%. Terminating all processes.")

                # Kill child processes
                children = parent.children(recursive=True)
                for child in children:
                    try:
                        print(f"Killing child PID {child.pid} ({child.name()})")
                        child.kill()
                        child.wait(timeout=5)
                    except Exception as e:
                        print(f"Could not kill PID {child.pid}: {e}")

                # Kill parent process (this script)
                print("Killing parent process.")
                os.kill(os.getpid(), termination_signal)
                break

            time.sleep(check_interval)
    
    # Start memory monitor in background
    return threading.Thread(target=_monitor_memory, daemon=True).start()




# %%
