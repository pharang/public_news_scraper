import errno
import signal
import os
from functools import wraps

class TimeoutError(Exception):
    # timeout 시 에러 발생
    pass

def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    """seconds초 이상 걸릴 경우 timeout 에러 발생"""
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.setitimer(signal.ITIMER_REAL,seconds) #used timer instead of alarm
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        return wraps(func)(wrapper)
    return decorator
