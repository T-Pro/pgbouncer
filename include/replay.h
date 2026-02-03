/*
 * PgBouncer - Lightweight connection pooler for PostgreSQL.
 *
 * Copyright (c) 2007-2009  Marko Kreen, Skype Technologies OÜ
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/*
 * Replay support - for mirroring queries to secondary servers.
 */

#pragma once

/* Check if pool has any replay hosts configured */
bool pool_has_replay(PgPool *pool);

/* Calculate the replay pool size based on replay host count */
int replay_pool_size(PgPool *pool);

/* Count replay server connections for the pool */
int replay_server_count(PgPool *pool);

/* Launch a new replay connection for the given host index */
void launch_replay_connection(PgPool *pool, int host_index);

/* Change state of a replay server */
void change_replay_server_state(PgSocket *server, SocketState newstate);

/* Release a replay server back to idle state */
void release_replay_server(PgSocket *server);

/* Get an idle replay server for the given host index */
PgSocket *get_idle_replay_server(PgPool *pool, int host_index);

/* Get an active replay server in transaction for the given host index */
PgSocket *get_active_replay_server_in_transaction(PgPool *pool, int host_index);

/* Check if the replay queue is full (global limit) */
bool replay_queue_full(void);

/* Get current global replay queue count */
int replay_queue_count(void);

/* Free a replay queue entry */
void replay_queue_free_entry(ReplayQueueEntry *entry);

/* Enqueue a query for replay */
bool replay_queue_enqueue(PgPool *pool, const void *data, int data_len, int host_index, bool is_transaction_end);

/* Dequeue the next replay entry for a given host index */
ReplayQueueEntry *replay_queue_dequeue(PgPool *pool, int host_index);

/* Dequeue the oldest replay entry from the pool */
ReplayQueueEntry *replay_queue_dequeue_any(PgPool *pool);

/* Clear all entries from a pool's replay queue */
void replay_queue_clear(PgPool *pool);

/* Process the replay queue for a pool */
int process_replay_queue(PgPool *pool);

/* Process replay queues for all pools */
void process_all_replay_queues(void);

/* Initialize replay support for a pool */
void replay_init_pool(PgPool *pool);

/* Cleanup replay resources for a pool */
void replay_cleanup_pool(PgPool *pool);
