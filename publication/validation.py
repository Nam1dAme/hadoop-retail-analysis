"""Bounded offline scans; online readers retain their independent five-second budget."""
def prepare_offline_checks(db):
    # Let MariaDB cancel the scan before the client socket times out.
    db._read_timeout=300
    with db.cursor() as q:q.execute('SET SESSION max_statement_time=240')
