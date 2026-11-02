"""Bounded pool; credentials are always the dedicated read-only role."""
from contextlib import contextmanager
import queue
import threading
from orchestration.database import connect


class Pool:
    def __init__(self, config, credentials, maximum=10):
        self.config, self.credentials = config, credentials
        self.idle = queue.LifoQueue(maximum)
        self.slots = threading.BoundedSemaphore(maximum)

    @contextmanager
    def connection(self):
        if not self.slots.acquire(timeout=5):
            raise TimeoutError('Read-only connection pool is busy')
        db = None
        try:
            try:
                db = self.idle.get_nowait()
                db.ping(reconnect=True)
            except queue.Empty:
                db = connect(self.config, self.credentials)
            # Reconnection creates a new server session and discards session variables.
            db._read_timeout = 5
            with db.cursor() as q:
                q.execute('SET SESSION max_statement_time=5')
            yield db
        except BaseException:
            if db:
                db.close()
                db = None
            raise
        finally:
            if db:
                self.idle.put_nowait(db)
            self.slots.release()

    def all(self, sql, args=()):
        with self.connection() as db, db.cursor() as q:
            q.execute(sql, args)
            return list(q.fetchall())

    def one(self, sql, args=()):
        rows = self.all(sql, args)
        return rows[0] if rows else None
