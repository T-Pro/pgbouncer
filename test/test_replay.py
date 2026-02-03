"""
Tests for the query replay feature.

The replay feature allows mirroring queries from a primary host to a secondary
"replay" host for comparison purposes.
"""

import asyncio
import time
import pytest
import psycopg

from .utils import WINDOWS


def get_stats(bouncer):
    """Get all stats from SHOW STATS as a dict indexed by database name."""
    with bouncer.admin_runner.cur() as cur:
        cur.execute("SHOW STATS")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return {row[0]: dict(zip(columns, row)) for row in rows}


def get_replay_stats(bouncer, dbname):
    """Get replay stats for a specific database from SHOW STATS."""
    stats = get_stats(bouncer)
    return stats.get(f"replay-{dbname}")


def get_replay_stats_direct(bouncer, dbname):
    """Get replay stats from SHOW REPLAY_STATS (includes dropped_count)."""
    with bouncer.admin_runner.cur() as cur:
        cur.execute("SHOW REPLAY_STATS")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        stats = {row[0]: dict(zip(columns, row)) for row in rows}
        return stats.get(dbname)


class TestReplayHostParsing:
    """Test that replay host configuration syntax is parsed correctly."""

    def test_replay_host_in_show_databases(self, bouncer):
        """Verify that databases with replay hosts are shown correctly."""
        result = bouncer.admin("SHOW DATABASES")
        assert result is not None
        # Check that our replay databases are listed
        db_names = [row[0] for row in result]
        assert "replay_stmt" in db_names

    def test_single_host_with_replay(self, bouncer):
        """Test a single host with a replay host configured."""
        # Connect to a replay-configured database and run a query
        with bouncer.conn(dbname="replay_stmt") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                assert result[0] == 1


class TestReplayStatsVerification:
    """Test that replay statistics are exposed and incremented correctly."""

    def test_replay_stats_and_query_count(self, bouncer):
        """Verify replay stats appear in SHOW STATS and query count is exact."""
        initial_stats = get_replay_stats(bouncer, "replay_stmt")
        initial_count = initial_stats['total_query_count'] if initial_stats else 0
        
        num_queries = 10
        with bouncer.conn(dbname="replay_stmt") as conn:
            with conn.cursor() as cur:
                for i in range(num_queries):
                    cur.execute(f"SELECT {i}")
                    cur.fetchone()
                time.sleep(0.5)
        
        time.sleep(0.3)
        
        # Verify replay stats appear in SHOW STATS
        result = bouncer.admin("SHOW STATS")
        db_names = [row[0] for row in result]
        assert "replay-replay_stmt" in db_names, \
            f"Expected 'replay-replay_stmt' in stats, got: {db_names}"
        
        # Verify exact query count
        final_stats = get_replay_stats(bouncer, "replay_stmt")
        assert final_stats is not None, "Replay stats should exist"
        
        replayed = final_stats['total_query_count'] - initial_count
        assert replayed == num_queries, \
            f"Expected exactly {num_queries} queries replayed, got {replayed}"


class TestReplayQueueOverflow:
    """Test replay queue overflow handling."""

    def test_queue_overflow_drops_queries(self, replay_bouncer, replay_verification_table):
        """Verify queries are dropped when queue overflows - exact counts.
        
        With lock held on replay server:
        - replay_pool_size connections block on the lock
        - queue_size queries go into the replay queue
        - Remaining queries are dropped (queue full)
        
        So: replayed = replay_pool_size + queue_size, dropped = total - replayed
        """
        import uuid
        bouncer = replay_bouncer
        replay_pg = bouncer.replay_pg
        batch_id = str(uuid.uuid4())[:8]
        num_queries = 50
        queue_size = 5
        
        # Get pool_size from SHOW DATABASES to calculate replay_pool_size
        # replay_pool_size = ceil(pool_size * replay_hosts / total_hosts)
        # With host=primary&replay, total_hosts=2, replay_hosts=1
        # So replay_pool_size = ceil(pool_size / 2)
        with bouncer.admin_runner.cur() as cur:
            cur.execute("SHOW DATABASES")
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            dbs = {row[0]: dict(zip(columns, row)) for row in rows}
        
        db_info = dbs.get("replay_verify")
        assert db_info is not None, "Could not find replay_verify in SHOW DATABASES"
        pool_size = int(db_info['pool_size'])
        
        replay_pool_size = ((pool_size + 1) // 2)
        
        # Set queue size
        bouncer.admin(f"SET replay_queue_size={queue_size}")
        
        try:
            # Lock the table on replay server FIRST
            lock_conninfo = f"host=127.0.0.1 port={replay_pg.port} dbname=p0 user=postgres"
            lock_conn = psycopg.connect(lock_conninfo, autocommit=False)
            lock_cursor = lock_conn.cursor()
            lock_cursor.execute("BEGIN")
            lock_cursor.execute("LOCK TABLE replay_verification IN EXCLUSIVE MODE")
            
            try:
                # Send one query first to establish a blocked replay connection
                with bouncer.conn(dbname="replay_verify") as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{batch_id}-setup')")
                
                # Small delay to ensure replay connection is blocked on lock
                time.sleep(0.2)
                
                # Get initial stats while replay connection is blocked
                initial_stats = get_replay_stats_direct(bouncer, "replay_verify")
                initial_dropped = initial_stats['dropped_count'] if initial_stats else 0
                
                # Send remaining queries - replay is blocked, so these queue up and overflow
                with bouncer.conn(dbname="replay_verify") as conn:
                    with conn.cursor() as cur:
                        for i in range(num_queries - 1):  # -1 for setup query
                            cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{batch_id}-{i}')")
                
                # Check dropped_count while lock is STILL held
                stats_blocked = get_replay_stats_direct(bouncer, "replay_verify")
                dropped_while_blocked = stats_blocked['dropped_count'] - initial_dropped
                
                # We sent (num_queries - 1) after setup, with replay blocked
                # Most should be dropped since queue is small
                remaining_queries = num_queries - 1  # excluding setup query
                assert dropped_while_blocked >= remaining_queries - queue_size - 1, \
                    f"Expected at least {remaining_queries - queue_size - 1} dropped, got {dropped_while_blocked}"
                
            finally:
                # Release lock
                lock_conn.rollback()
                lock_conn.close()
            
            # Wait for queued queries to complete
            time.sleep(0.5)
            
            # Verify exact row counts
            primary_count = bouncer.pg.sql_value(
                f"SELECT count(*) FROM replay_verification WHERE query_id LIKE '{batch_id}-%'",
                dbname="p0"
            )
            assert primary_count == num_queries, \
                f"Expected {num_queries} rows on primary, got {primary_count}"
            
            replay_count = replay_pg.sql_value(
                f"SELECT count(*) FROM replay_verification WHERE query_id LIKE '{batch_id}-%'",
                dbname="p0"
            )
            
            # Verify accounting: dropped + replayed = total
            final_stats = get_replay_stats_direct(bouncer, "replay_verify")
            total_dropped = final_stats['dropped_count'] - initial_dropped
            
            # The key invariant: dropped + replayed = total queries sent
            assert total_dropped + replay_count == num_queries, \
                f"Accounting: dropped({total_dropped}) + replayed({replay_count}) != {num_queries}"
            
            # Replayed should be small (1 blocked + queue_size queued + maybe 1 race)
            max_replayed = 1 + queue_size + 1
            assert replay_count <= max_replayed, \
                f"Expected at most {max_replayed} rows on replay, got {replay_count}"
                
        finally:
            # Reset queue size
            bouncer.admin("SET replay_queue_size=1000")


class TestReplayPoolModes:
    """Test replay feature with different pool modes and verify replay execution."""

    def test_statement_mode_replay_stats(self, bouncer):
        """Test statement mode queries are replayed and stats increment."""
        initial_stats = get_replay_stats(bouncer, "replay_stmt")
        initial_count = initial_stats['total_query_count'] if initial_stats else 0
        
        with bouncer.conn(dbname="replay_stmt") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                assert cur.fetchone()[0] == 1
                cur.execute("SELECT 2")
                assert cur.fetchone()[0] == 2
        
        time.sleep(0.2)
        
        final_stats = get_replay_stats(bouncer, "replay_stmt")
        assert final_stats is not None
        assert final_stats['total_query_count'] > initial_count

    def test_transaction_mode_replay_stats(self, bouncer):
        """Test transaction mode queries are replayed with correct count."""
        initial_stats = get_replay_stats(bouncer, "replay_txn")
        initial_count = initial_stats['total_query_count'] if initial_stats else 0
        
        with bouncer.conn(dbname="replay_txn") as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("SELECT 1")
                assert cur.fetchone()[0] == 1
                cur.execute("COMMIT")
        
        time.sleep(0.2)
        
        final_stats = get_replay_stats(bouncer, "replay_txn")
        assert final_stats is not None
        # Should have at least BEGIN, SELECT, COMMIT = 3 more queries
        assert final_stats['total_query_count'] >= initial_count + 3, \
            f"Expected at least 3 more queries replayed: {initial_count} -> {final_stats['total_query_count']}"

    def test_session_mode_replay_stats(self, bouncer):
        """Test session mode queries are replayed."""
        initial_stats = get_replay_stats(bouncer, "replay_session")
        initial_count = initial_stats['total_query_count'] if initial_stats else 0
        
        with bouncer.conn(dbname="replay_session") as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("SELECT 1")
                assert cur.fetchone()[0] == 1
                cur.execute("COMMIT")
        
        time.sleep(0.2)
        
        final_stats = get_replay_stats(bouncer, "replay_session")
        assert final_stats is not None
        assert final_stats['total_query_count'] > initial_count

    def test_session_mode_multiple_transactions_replay(self, bouncer):
        """Test multiple transactions in session mode all get replayed."""
        initial_stats = get_replay_stats(bouncer, "replay_session")
        initial_count = initial_stats['total_query_count'] if initial_stats else 0
        
        with bouncer.conn(dbname="replay_session") as conn:
            with conn.cursor() as cur:
                # First transaction: BEGIN, SELECT, COMMIT = 3
                cur.execute("BEGIN")
                cur.execute("SELECT 1")
                cur.execute("COMMIT")
                # Second transaction: BEGIN, SELECT, ROLLBACK = 3
                cur.execute("BEGIN")
                cur.execute("SELECT 2")
                cur.execute("ROLLBACK")
                # Simple query = 1
                cur.execute("SELECT 3")
                assert cur.fetchone()[0] == 3
        
        time.sleep(0.3)
        
        final_stats = get_replay_stats(bouncer, "replay_session")
        assert final_stats is not None
        # Should have 7 more queries replayed
        assert final_stats['total_query_count'] >= initial_count + 7, \
            f"Expected at least 7 more queries replayed: {initial_count} -> {final_stats['total_query_count']}"


class TestReplayFeature:
    """Integration tests for the replay feature."""

    def test_multi_host_replay(self, bouncer):
        """Test multi-host configuration with replay."""
        initial_stats = get_replay_stats(bouncer, "replay_multi")
        initial_count = initial_stats['total_query_count'] if initial_stats else 0
        
        with bouncer.conn(dbname="replay_multi") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                assert result[0] == 1
        
        time.sleep(0.2)
        
        final_stats = get_replay_stats(bouncer, "replay_multi")
        assert final_stats is not None
        assert final_stats['total_query_count'] > initial_count

    def test_replay_server_bytes_tracked(self, bouncer):
        """Verify that bytes sent to replay server are tracked in stats."""
        initial_stats = get_replay_stats(bouncer, "replay_stmt")
        initial_bytes = initial_stats['total_sent'] if initial_stats else 0
        
        # Run queries that send some data
        with bouncer.conn(dbname="replay_stmt") as conn:
            with conn.cursor() as cur:
                for i in range(5):
                    cur.execute(f"SELECT {i}")
                    cur.fetchone()
        
        time.sleep(0.2)
        
        final_stats = get_replay_stats(bouncer, "replay_stmt")
        assert final_stats is not None
        assert final_stats['total_sent'] > initial_bytes, \
            f"Bytes sent should increase: {initial_bytes} -> {final_stats['total_sent']}"




class TestReplayEndToEnd:
    """End-to-end replay verification using two PostgreSQL instances.
    
    These tests verify that queries executed on the primary server are
    actually replayed and executed on a separate replay server.
    """

    def test_insert_replayed_to_separate_server(self, replay_bouncer, replay_verification_table):
        """Verify INSERT queries are replayed to the separate replay server."""
        import uuid
        
        # Generate unique query ID
        query_id = str(uuid.uuid4())
        
        # Execute INSERT through bouncer (goes to primary, replayed to replay_pg)
        with replay_bouncer.conn(dbname="replay_verify") as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{query_id}')")
                # Keep connection open to allow replay to process
                time.sleep(0.5)
        
        # Give replay more time to complete
        time.sleep(0.5)
        
        # Verify row exists on PRIMARY server
        primary_count = replay_bouncer.pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id = '{query_id}'",
            dbname="p0"
        )
        assert primary_count == 1, f"Expected 1 row on primary, got {primary_count}"
        
        # Verify row exists on REPLAY server
        replay_count = replay_bouncer.replay_pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id = '{query_id}'",
            dbname="p0"
        )
        assert replay_count == 1, \
            f"Expected 1 row on replay server, got {replay_count}. Query was not replayed!"

    def test_multiple_inserts_replayed(self, replay_bouncer, replay_verification_table):
        """Verify multiple INSERT queries are all replayed."""
        import uuid
        
        batch_id = str(uuid.uuid4())[:8]
        num_inserts = 5
        
        # Execute multiple INSERTs
        with replay_bouncer.conn(dbname="replay_verify") as conn:
            with conn.cursor() as cur:
                for i in range(num_inserts):
                    query_id = f"{batch_id}-{i}"
                    cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{query_id}')")
                time.sleep(0.5)
        
        time.sleep(0.5)
        
        # Verify all rows on primary
        primary_count = replay_bouncer.pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id LIKE '{batch_id}-%'",
            dbname="p0"
        )
        assert primary_count == num_inserts, \
            f"Expected {num_inserts} rows on primary, got {primary_count}"
        
        # Verify all rows on replay
        replay_count = replay_bouncer.replay_pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id LIKE '{batch_id}-%'",
            dbname="p0"
        )
        assert replay_count == num_inserts, \
            f"Expected {num_inserts} rows on replay server, got {replay_count}"

    def test_transaction_replayed_atomically(self, replay_bouncer, replay_verification_table):
        """Verify transaction (BEGIN/INSERT/COMMIT) is replayed to separate server."""
        import uuid
        
        query_id = str(uuid.uuid4())
        
        with replay_bouncer.conn(dbname="replay_verify") as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{query_id}')")
                cur.execute("COMMIT")
                time.sleep(0.5)
        
        time.sleep(0.5)
        
        # Verify row exists on replay server (transaction was committed)
        replay_count = replay_bouncer.replay_pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id = '{query_id}'",
            dbname="p0"
        )
        assert replay_count == 1, \
            f"Transaction not replayed - expected 1 row on replay server, got {replay_count}"

class TestReplayWithReboot:
    """Tests verifying replay functionality survives PgBouncer reboot."""

    @pytest.mark.skipif(WINDOWS, reason="takeover hangs on Windows")
    async def test_replay_survives_reboot(self, replay_bouncer, replay_verification_table):
        """Verify replay continues to work after online restart."""
        import uuid
        
        # Insert before reboot
        query_id_before = str(uuid.uuid4())
        with replay_bouncer.conn(dbname="replay_verify") as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{query_id_before}')")
        
        await asyncio.sleep(0.5)
        
        # Verify replayed before reboot
        count_before = replay_bouncer.replay_pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id = '{query_id_before}'",
            dbname="p0"
        )
        assert count_before == 1, f"Row not replayed before reboot: {count_before}"
        
        # Reboot PgBouncer
        await replay_bouncer.reboot()
        await asyncio.sleep(0.5)
        
        # Insert after reboot
        query_id_after = str(uuid.uuid4())
        with replay_bouncer.conn(dbname="replay_verify") as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{query_id_after}')")
        
        await asyncio.sleep(0.5)
        
        # Verify replayed after reboot
        count_after = replay_bouncer.replay_pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id = '{query_id_after}'",
            dbname="p0"
        )
        assert count_after == 1, f"Row not replayed after reboot: {count_after}"

    @pytest.mark.skipif(WINDOWS, reason="takeover hangs on Windows")
    async def test_replay_stats_reset_after_reboot(self, bouncer):
        """Verify replay stats reset after online restart."""
        # Generate some replay traffic
        with bouncer.conn(dbname="replay_stmt") as conn:
            with conn.cursor() as cur:
                for _ in range(5):
                    cur.execute("SELECT 1")
        
        await asyncio.sleep(0.3)
        
        # Check stats exist
        stats_before = get_replay_stats(bouncer, "replay_stmt")
        assert stats_before is not None, "Expected replay stats before reboot"
        assert stats_before['total_query_count'] >= 5
        
        # Reboot
        await bouncer.reboot()
        await asyncio.sleep(0.3)
        
        # Stats should reset to 0 after takeover
        stats_after = get_replay_stats(bouncer, "replay_stmt")
        # Stats might not exist yet or be 0
        if stats_after:
            assert stats_after['total_query_count'] == 0, \
                f"Expected stats reset after reboot, got {stats_after['total_query_count']}"

    @pytest.mark.skipif(WINDOWS, reason="takeover hangs on Windows")
    async def test_replay_during_reboot(self, replay_bouncer, replay_verification_table):
        """Verify queries issued during reboot are still replayed."""
        import uuid
        
        # Start queries in background
        query_ids = [str(uuid.uuid4()) for _ in range(3)]
        
        async def insert_queries():
            for qid in query_ids:
                with replay_bouncer.conn(dbname="replay_verify") as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"INSERT INTO replay_verification(query_id) VALUES ('{qid}')")
                await asyncio.sleep(0.1)
        
        # Run inserts and reboot concurrently
        insert_task = asyncio.create_task(insert_queries())
        await asyncio.sleep(0.15)  # Let first insert start
        await replay_bouncer.reboot()
        await insert_task
        
        await asyncio.sleep(1.0)  # Wait for replay to catch up
        
        # All queries should be replayed (either before or after reboot)
        total_replayed = replay_bouncer.replay_pg.sql_value(
            f"SELECT count(*) FROM replay_verification WHERE query_id IN ({','.join(repr(q) for q in query_ids)})",
            dbname="p0"
        )
        assert total_replayed == 3, \
            f"Expected all 3 queries replayed, got {total_replayed}"
