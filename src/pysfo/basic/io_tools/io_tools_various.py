

def silent_call(func, *args, verbose=True, **kwargs):
    """
    Calls a function while optionally suppressing all print and stderr output.
    
    Parameters
    ----------
    func : callable
        The function to execute.
    *args, **kwargs :
        Positional and keyword arguments for `func`.
    verbose : bool, default True
        If False, suppresses stdout and stderr output during execution.
    
    Returns
    -------
    Any
        The return value of `func`.
    """

    import io
    from contextlib import redirect_stdout, redirect_stderr

    if verbose:
        return func(*args, **kwargs)
    
    with io.StringIO() as buf_out, io.StringIO() as buf_err, \
         redirect_stdout(buf_out), redirect_stderr(buf_err):
        return func(*args, **kwargs)
    
def log_listener(log_queue, log_path=None, total=None):
    """
    Prints messages from queue and optionally tracks how many have completed.
    """
    count = 0
    file_handle = open(log_path, "a", encoding="utf-8") if log_path else None

    try:
        while True:
            msg = log_queue.get()
            if msg == "DONE":
                break

            if msg.startswith("[OK]") or msg.startswith("[SKIP]"):
                count += 1
                progress = f"({count}/{total}) " if total else ""
            else:
                progress = ""

            full_msg = f"{progress}{msg}"
            print(full_msg, flush=True)

            if file_handle:
                file_handle.write(full_msg + "\n")
                file_handle.flush()
    finally:
        if file_handle:
            file_handle.close()

def is_interactive_session() -> bool:
    """Return True if running in an interactive (IPython/VSCode kernel) session."""

    import sys, os

    try:
        from IPython import get_ipython
        if get_ipython() is not None:
            return True
    except ImportError:
        pass

    if hasattr(sys, 'ps1') or sys.flags.interactive:
        return True

    if "VSCODE_PID" in os.environ or "JPY_PARENT_PID" in os.environ:
        return True

    return False