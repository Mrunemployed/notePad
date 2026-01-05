from fastapi import HTTPException
from typing import Callable
from functools import wraps
import logging
import json
import sys

logger = logging.getLogger("app.exceptions")

def exceptionHandler(func: Callable):
    @wraps(func)
    async def routing(*args, **kwargs):
        route_name = f"{func.__module__}.{func.__name__}"
        try:
            res = await func(*args, **kwargs)
            return res
        except HTTPException as httpex:
            # Keep behavior the same while logging a lightweight summary
            logger.info(
                "HTTPException %s in %s: %s",
                httpex.status_code,
                route_name,
                str(httpex.detail)[:300],
            )
            try:
                detail = json.loads(httpex.detail)  # preserve existing behavior
            except Exception:
                # Re-raise as is
                raise HTTPException(status_code=httpex.status_code, detail=httpex.detail)
            raise HTTPException(status_code=httpex.status_code, detail=detail)

        except Exception as Err:
            # Lightweight source details without formatting entire traceback
            tb = Err.__traceback__
            last_tb = tb
            while last_tb and last_tb.tb_next:
                last_tb = last_tb.tb_next

            if last_tb is not None:
                filename = last_tb.tb_frame.f_code.co_filename
                lineno = last_tb.tb_lineno
                func_name = last_tb.tb_frame.f_code.co_name
            else:
                filename = None
                lineno = None
                func_name = None

            logger.error(
                "Unhandled exception in %s at %s:%s in %s: %s: %s",
                route_name,
                filename,
                lineno,
                func_name,
                type(Err).__name__,
                Err,
            )

            # Optionally include stack only when debugging is enabled
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Stack trace (debug)")

            # Keep outward HTTP response behavior unchanged
            raise HTTPException(
                status_code=400,
                detail={
                    'errors': "Server Error: Invalid Request",
                    'message': "A server exception occurred",
                },
            )
    return routing
