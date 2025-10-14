

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