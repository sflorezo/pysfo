

def is_nested_parallel() -> bool:
    from joblib.parallel import get_active_backend
    backend, _ = get_active_backend()
    # KEY: uncomment the following line to see what is happening in the backend when debugging
    print(type(backend), getattr(backend, "nesting_level", None), getattr(backend, "n_jobs", None))
    return getattr(backend, "nesting_level", 0) > 0