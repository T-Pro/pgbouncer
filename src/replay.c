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

#include "bouncer.h"

#include <usual/err.h>
#include <usual/slab.h>
#include <usual/socket.h>
#include <usual/string.h>

/* External dependencies from objects.c */
extern struct Slab *server_cache;
extern struct StatList pool_list;
extern struct DNSContext *adns;

/* Global count of queued replay entries across all pools */
static int global_replay_queue_count = 0;

/*
 * Check if pool has any replay hosts configured
 */
bool pool_has_replay(PgPool *pool)
{
	if (!pool || !pool->db || !pool->db->host_pool)
		return false;
	return hostpool_has_replay(pool->db->host_pool);
}

/*
 * Get the host for a given host index from the host pool.
 * host_index is 1-based in PgSocket, converts to 0-based array index.
 * Returns NULL if index is out of range.
 */
static PgHost *hostpool_get_host_by_index(PgPool *pool, int host_index)
{
	PgHostPool *hp;

	if (!pool || !pool->db)
		return NULL;

	hp = pool->db->host_pool;
	if (!hp)
		return NULL;

	/* host_index is 1-based in PgSocket, convert to 0-based array index */
	if (host_index <= 0 || host_index > hp->count)
		return NULL;

	return hp->hosts[host_index - 1];
}

/*
 * Connect to a replay host for the given server's host.
 * Uses same address resolution as dns_connect() in objects.c.
 */
static void dns_connect_replay(struct PgSocket *server, const char *replay_host, int replay_port)
{
	struct sockaddr_in sa_in;
	struct sockaddr *sa;
	int sa_len;
	int res;

	if (!replay_host || !*replay_host) {
		disconnect_server(server, false, "no replay host configured");
		return;
	}

	server->host = xstrdup(replay_host);

	/* Parse IPv4 address */
	slog_noise(server, "replay socket: %s:%d", replay_host, replay_port);
	memset(&sa_in, 0, sizeof(sa_in));
	sa_in.sin_family = AF_INET;
	res = inet_pton(AF_INET, replay_host, &sa_in.sin_addr);
	sa_in.sin_port = htons(replay_port);
	sa = (struct sockaddr *)&sa_in;
	sa_len = sizeof(sa_in);

	/* if simple parse failed, use DNS */
	if (res != 1) {
		struct DNSToken *tk;
		slog_noise(server, "replay dns lookup: %s", replay_host);
		tk = adns_resolve(adns, replay_host, dns_callback, server);
		if (tk)
			server->dns_token = tk;
		return;
	}

	connect_server(server, sa, sa_len);
}

/*
 * Calculate the replay pool size based on the number of replay hosts
 */
int replay_pool_size(PgPool *pool)
{
	int total_hosts;
	int replay_hosts;
	int pool_size;

	if (!pool || !pool->db || !pool->db->host_pool)
		return 0;

	total_hosts = pool->db->host_pool->count;
	replay_hosts = hostpool_replay_count(pool->db->host_pool);

	if (replay_hosts == 0 || total_hosts == 0)
		return 0;

	pool_size = pool_pool_size(pool);
	/* Proportional size: ceil(pool_size * replay_hosts / total_hosts) */
	return (pool_size * replay_hosts + total_hosts - 1) / total_hosts;
}

/*
 * Count replay server connections for the pool
 */
int replay_server_count(PgPool *pool)
{
	return statlist_count(&pool->replay_idle_server_list) +
	       statlist_count(&pool->replay_active_server_list) +
	       statlist_count(&pool->replay_new_server_list);
}

/*
 * Launch a new replay connection for the given host index.
 * host_index is 1-based (matching the convention in PgSocket).
 */
void launch_replay_connection(PgPool *pool, int host_index)
{
	PgSocket *server;
	PgHost *host;
	int max_replay;

	if (!pool_has_replay(pool))
		return;

	host = hostpool_get_host_by_index(pool, host_index);
	if (!host || !host->replay_hostname)
		return;

	/* Check if we already have a connection attempt in progress */
	if (!statlist_empty(&pool->replay_new_server_list)) {
		log_debug("launch_replay_connection: already in progress");
		return;
	}

	/* if replay server bounces, don't retry too fast */
	if (pool->replay_last_connect_failed) {
		usec_t now = get_cached_time();
		if (now - pool->replay_last_connect_time < cf_server_login_retry) {
			log_debug("launch_replay_connection: last failed, waiting");
			return;
		}
	}

	/* Check replay pool size limit */
	max_replay = replay_pool_size(pool);
	if (max_replay > 0 && replay_server_count(pool) >= max_replay) {
		log_debug("launch_replay_connection: replay pool full (%d >= %d)",
			  replay_server_count(pool), max_replay);
		return;
	}

	/* Allocate server socket */
	server = slab_alloc(server_cache);
	if (!server) {
		log_debug("launch_replay_connection: no memory");
		return;
	}

	/* Initialize it as a replay connection */
	server->pool = pool;
	server->login_user_credentials = pool->user_credentials;
	server->is_replay = true;
	server->replay_host_index = host_index;
	server->host_index = host_index;
	server->connect_time = get_cached_time();
	statlist_init(&server->canceling_clients, "canceling_clients");

	pool->replay_last_connect_time = get_cached_time();

	/* Add to replay new server list (both pool-level and socketpool) */
	statlist_append(&pool->replay_new_server_list, &server->head);
	if (pool->socket_pool)
		socketpool_add_replay_new(pool->socket_pool, server);
	server->state = SV_LOGIN;

	dns_connect_replay(server, host->replay_hostname, host->replay_port);
}

/*
 * Change state of a replay server.
 * Updates both pool-level lists and per-host socketpool lists.
 */
void change_replay_server_state(PgSocket *server, SocketState newstate)
{
	PgPool *pool = server->pool;
	PgSocketPool *sp = pool->socket_pool;

	if (!server->is_replay)
		return;

	/* Remove from current list based on old state */
	switch (server->state) {
	case SV_LOGIN:
		statlist_remove(&pool->replay_new_server_list, &server->head);
		if (sp)
			socketpool_remove_replay_new(sp, server);
		break;
	case SV_IDLE:
		statlist_remove(&pool->replay_idle_server_list, &server->head);
		if (sp)
			socketpool_remove_replay_idle(sp, server);
		break;
	case SV_ACTIVE:
		statlist_remove(&pool->replay_active_server_list, &server->head);
		if (sp)
			socketpool_dec_replay_active(sp, server->replay_host_index);
		break;
	default:
		break;
	}

	server->state = newstate;

	/* Add to new list based on new state */
	switch (newstate) {
	case SV_LOGIN:
		statlist_append(&pool->replay_new_server_list, &server->head);
		if (sp)
			socketpool_add_replay_new(sp, server);
		break;
	case SV_IDLE:
		statlist_append(&pool->replay_idle_server_list, &server->head);
		if (sp)
			socketpool_add_replay_idle(sp, server);
		break;
	case SV_ACTIVE:
		statlist_append(&pool->replay_active_server_list, &server->head);
		if (sp)
			socketpool_inc_replay_active(sp, server->replay_host_index);
		break;
	case SV_FREE:
	case SV_JUSTFREE:
		/* Server is being freed, don't add to any list */
		break;
	default:
		break;
	}
}

/*
 * Release a replay server back to idle state
 */
void release_replay_server(PgSocket *server)
{
	if (!server->is_replay)
		return;

	server->ready = true;
	server->replay_in_transaction = false;
	change_replay_server_state(server, SV_IDLE);

	/* Process queued entries now that server is idle */
	process_replay_queue(server->pool);
}

/*
 * Get an idle replay server for the given host index.
 * Uses O(1) lookup via socketpool when available, falls back to O(n) scan.
 */
PgSocket *get_idle_replay_server(PgPool *pool, int host_index)
{
	struct List *item;
	PgSocket *server;

	/* O(1) lookup via socket pool when available */
	if (pool->socket_pool && pool->socket_pool->replay_idle_lists) {
		return socketpool_get_replay_idle(pool->socket_pool, host_index);
	}

	/* Fallback: O(n) scan of pool-level list */
	statlist_for_each(item, &pool->replay_idle_server_list) {
		server = container_of(item, PgSocket, head);
		if (server->replay_host_index == host_index)
			return server;
	}
	return NULL;
}

/*
 * Get an active replay server that is in a transaction for the given host index.
 * This allows sending subsequent transaction queries to the same server.
 */
PgSocket *get_active_replay_server_in_transaction(PgPool *pool, int host_index)
{
	struct List *item;
	PgSocket *server;

	statlist_for_each(item, &pool->replay_active_server_list) {
		server = container_of(item, PgSocket, head);
		if (server->replay_host_index == host_index && server->replay_in_transaction)
			return server;
	}
	return NULL;
}

/*
 * Check if the replay queue is full (global limit)
 */
bool replay_queue_full(void)
{
	return cf_replay_queue_size > 0 && global_replay_queue_count >= cf_replay_queue_size;
}

/*
 * Get current global replay queue count
 */
int replay_queue_count(void)
{
	return global_replay_queue_count;
}

/*
 * Free a replay queue entry
 */
void replay_queue_free_entry(ReplayQueueEntry *entry)
{
	if (!entry)
		return;
	free(entry->data);
	free(entry);
}

/*
 * Allocate and enqueue a query for replay.
 * Returns true if successfully queued, false if queue is full or allocation fails.
 */
bool replay_queue_enqueue(PgPool *pool, const void *data, int data_len, int host_index, bool is_transaction_end)
{
	ReplayQueueEntry *entry;

	if (!pool || !data || data_len <= 0)
		return false;

	/* Check global queue limit */
	if (replay_queue_full()) {
		pool->replay_stats.dropped_count++;
		return false;
	}

	entry = malloc(sizeof(ReplayQueueEntry));
	if (!entry) {
		pool->replay_stats.dropped_count++;
		return false;
	}

	entry->data = malloc(data_len);
	if (!entry->data) {
		free(entry);
		pool->replay_stats.dropped_count++;
		return false;
	}

	memcpy(entry->data, data, data_len);
	entry->data_len = data_len;
	entry->host_index = host_index;
	entry->queued_time = get_cached_time();
	entry->is_transaction_end = is_transaction_end;
	list_init(&entry->node);

	/* Add to pool's replay queue */
	list_append(&pool->replay_queue, &entry->node);
	pool->replay_queue_count++;
	global_replay_queue_count++;

	/* Trigger queue processing to start replay connection if needed */
	process_replay_queue(pool);

	return true;
}

/*
 * Dequeue the next replay entry for a given host index.
 * Returns NULL if no entry is available for that host.
 */
ReplayQueueEntry *replay_queue_dequeue(PgPool *pool, int host_index)
{
	struct List *item, *tmp;
	ReplayQueueEntry *entry;

	if (!pool)
		return NULL;

	list_for_each_safe(item, &pool->replay_queue, tmp) {
		entry = container_of(item, ReplayQueueEntry, node);
		if (entry->host_index == host_index) {
			list_del(&entry->node);
			pool->replay_queue_count--;
			global_replay_queue_count--;
			return entry;
		}
	}
	return NULL;
}

/*
 * Dequeue the oldest replay entry from the pool, regardless of host index.
 * Returns NULL if queue is empty.
 */
ReplayQueueEntry *replay_queue_dequeue_any(PgPool *pool)
{
	struct List *item;
	ReplayQueueEntry *entry;

	if (!pool || list_empty(&pool->replay_queue))
		return NULL;

	item = pool->replay_queue.next;
	entry = container_of(item, ReplayQueueEntry, node);
	list_del(&entry->node);
	pool->replay_queue_count--;
	global_replay_queue_count--;

	return entry;
}

/*
 * Clear all entries from a pool's replay queue
 */
void replay_queue_clear(PgPool *pool)
{
	struct List *item, *tmp;
	ReplayQueueEntry *entry;

	if (!pool)
		return;

	list_for_each_safe(item, &pool->replay_queue, tmp) {
		entry = container_of(item, ReplayQueueEntry, node);
		list_del(&entry->node);
		global_replay_queue_count--;
		replay_queue_free_entry(entry);
	}
	pool->replay_queue_count = 0;
}

/*
 * Send a replay queue entry to a replay server.
 * Returns true if successfully sent, false otherwise.
 */
static bool send_replay_query(PgSocket *replay_server, ReplayQueueEntry *entry)
{
	PgPool *pool;
	bool res;

	if (!replay_server || !entry)
		return false;

	pool = replay_server->pool;

	/* Mark server as active */
	change_replay_server_state(replay_server, SV_ACTIVE);
	replay_server->ready = false;

	/* Track transaction state */
	if (entry->is_transaction_end) {
		replay_server->replay_in_transaction = false;
	} else {
		/* Simple heuristic: if we see a query, we might be in a transaction */
		replay_server->replay_in_transaction = true;
	}

	/* Send the query data to the replay server using sbuf_answer */
	res = sbuf_answer(&replay_server->sbuf, entry->data, entry->data_len);
	if (!res) {
		/* Failed to send, put server back to idle */
		change_replay_server_state(replay_server, SV_IDLE);
		return false;
	}

	/* Update replay stats */
	pool->replay_stats.query_count++;
	pool->replay_stats.server_bytes += entry->data_len;

	return true;
}

/*
 * Process the replay queue for a pool - send pending queries to idle replay servers.
 * Returns the number of queries processed.
 */
int process_replay_queue(PgPool *pool)
{
	int processed = 0;
	struct List *item, *tmp;
	ReplayQueueEntry *entry;
	PgSocket *replay_server;

	if (!pool || !pool_has_replay(pool))
		return 0;

	/*
	 * Process entries by host index to maintain transaction ordering.
	 * For each host index, we try to send as many queries as we can
	 * to the same server (if in transaction) or any idle server.
	 */
	list_for_each_safe(item, &pool->replay_queue, tmp) {
		entry = container_of(item, ReplayQueueEntry, node);

		/* Find an idle replay server for this host */
		replay_server = get_idle_replay_server(pool, entry->host_index);

		if (!replay_server) {
			/* No idle server available - try to launch one */
			launch_replay_connection(pool, entry->host_index);
			/* Can't process this entry yet, try next host's entries */
			continue;
		}

		/* Remove from queue */
		list_del(&entry->node);
		pool->replay_queue_count--;
		global_replay_queue_count--;

		/* Send the query to the replay server */
		if (!send_replay_query(replay_server, entry)) {
			/* Failed to send - drop the query */
			pool->replay_stats.dropped_count++;
		}

		replay_queue_free_entry(entry);
		processed++;
	}

	return processed;
}

/*
 * Process replay queues for all pools
 */
void process_all_replay_queues(void)
{
	struct List *item;
	PgPool *pool;

	statlist_for_each(item, &pool_list) {
		pool = container_of(item, PgPool, head);
		if (pool_has_replay(pool)) {
			process_replay_queue(pool);
		}
	}
}

/*
 * Initialize replay support for a pool
 */
void replay_init_pool(PgPool *pool)
{
	statlist_init(&pool->replay_idle_server_list, "replay_idle_server_list");
	statlist_init(&pool->replay_active_server_list, "replay_active_server_list");
	statlist_init(&pool->replay_new_server_list, "replay_new_server_list");
	list_init(&pool->replay_queue);
	pool->replay_queue_count = 0;
}

/*
 * Cleanup replay resources for a pool
 */
void replay_cleanup_pool(PgPool *pool)
{
	replay_queue_clear(pool);
}
