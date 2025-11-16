from importlib import import_module

class LazyModule:
    """
    Lightweight lazy loader for optional or heavy submodules.

    - Import errors are deferred until attribute access.
    - Does not affect package import.
    - Useful for optional dependencies, slow imports, or reducing load-time.
    """
    def __init__(self, module_name: str):
        self._module_name = module_name
        self._module = None

    def _load(self):
        if self._module is None:
            self._module = import_module(self._module_name)
        return self._module

    def __getattr__(self, item):
        module = self._load()
        return getattr(module, item)

    def __dir__(self):
        return dir(self._load())