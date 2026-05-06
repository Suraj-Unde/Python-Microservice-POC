from contextvars import ContextVar

correlation_id = ContextVar("correlation_id", default=None)

def set_correlation_id(cid):
    correlation_id.set(cid)

def get_correlation_id():
    return correlation_id.get()