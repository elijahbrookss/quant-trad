
OVERLAY_HANDLERS: dict[str, callable] = {}

def register_overlay_handler(kind: str):
    """
    Decorator to register a function as an overlay handler for a specific kind.
    """
    def decorator(func: callable):
        if kind in OVERLAY_HANDLERS:
            raise ValueError(f"Overlay handler for '{kind}' is already registered.")
        OVERLAY_HANDLERS[kind] = func
        return func
    return decorator

def get_overlay_handler(kind: str) -> callable:
    """
    Retrieve the overlay handler for a specific kind.
    Raises KeyError if no handler is registered for the kind.
    """
    if kind not in OVERLAY_HANDLERS:
        raise KeyError(f"No overlay handler registered for kind '{kind}'")
    return OVERLAY_HANDLERS[kind]
