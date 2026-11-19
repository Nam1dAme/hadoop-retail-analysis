"""Bounded pool; credentials are always the dedicated read-only role."""
from contextlib import contextmanager
import queue
import threading
import time
from orchestration.database import connect


class Pool:
    def __init__(self, config, credentials, maximum=10):
        self.config, self.credentials = config, credentials
        self.idle = queue.LifoQueue(maximum)
        self.slots = threading.BoundedSemaphore(maximum)

    @contextmanager
    def connection(self, acquire_timeout=5, statement_timeout=5):
        if not self.slots.acquire(timeout=acquire_timeout):
            raise TimeoutError('Read-only connection pool is busy')
        db = None
        try:
            try:
                db = self.idle.get_nowait()
                if not db.open or time.monotonic() - getattr(db, '_last_used', 0.0) > 1.0:
                    db.ping(reconnect=True)
            except queue.Empty:
                db = connect(self.config, self.credentials)
            target_timeout = max(0.001, float(statement_timeout))
            db._read_timeout = target_timeout + 0.25 if statement_timeout != 5 else 5
            # Only set session variable when the underlying socket changed (new/reconnected) or timeout differs.
            if db._sock is not getattr(db, '_last_sock', None) or getattr(db, '_max_statement_time', None) != target_timeout:
                with db.cursor() as q:
                    q.execute('SET SESSION max_statement_time=%s', (target_timeout,))
                db._last_sock = db._sock
                db._max_statement_time = target_timeout
            yield db
        except BaseException:
            if db:
                db.close()
                db = None
            raise
        finally:
            if db:
                db._last_used = time.monotonic()
                self.idle.put_nowait(db)
            self.slots.release()

    def all(self, sql, args=(), *, timeout=None):
        deadline = time.monotonic() + timeout if timeout is not None else None
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Read budget expired before acquiring connection')
            acquire_timeout = max(0.0, remaining)
            statement_timeout = remaining
        else:
            acquire_timeout = 5
            statement_timeout = 5
        with self.connection(acquire_timeout=acquire_timeout, statement_timeout=statement_timeout) as db, db.cursor() as q:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError('Read budget expired while acquiring connection')
            q.execute(sql, args)
            result = list(q.fetchall())
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError('Read budget expired')
            return result

    def one(self, sql, args=()):
        rows = self.all(sql, args)
        return rows[0] if rows else None
