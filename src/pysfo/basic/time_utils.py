#%%



def print_elapsed_every(interval=60, stop_event=None):

    import time

    start = time.time()
    while not stop_event.is_set():
        time.sleep(interval)
        elapsed = int(time.time() - start)
        print(f"{elapsed} seconds passed")

def run_with_timeout(func, args=(), kwargs=None, timeout=120):

    raise ValueError("'run_with_timeout' not implemented yet by SFO.")

    # FIXME: This function is not yet implemented. Need to add way to print outputs of func when running with timeout

    import multiprocessing as mp

    if kwargs is None:
        kwargs = {}
    p = mp.Process(target=func, args=args, kwargs=kwargs)
    p.start()
    p.join(timeout)  # wait up to timeout seconds
    if p.is_alive():
        print("Timeout reached, killing process...")
        p.terminate()
        p.join()
        return None
    return "Finished successfully"

def merge_with_progress(left, right, interval = 60, **merge_kwargs):

    import threading
    import pandas as pd

    stop_event = threading.Event()
    progress_thread = threading.Thread(target=print_elapsed_every, args=(interval, stop_event))
    progress_thread.start()

    try:
        merged = pd.merge(left, right, **merge_kwargs)
    finally:
        stop_event.set()
        progress_thread.join()

    return merged