// Package nqc implements Not Quite Consistency, a cache protocol for independent
// standalone Redis instances. Redis Pub/Sub is treated as a best-effort hint path;
// authoritative origin reads and verified shard snapshots are the only sources that may
// advance confirmed state.
//
// The package intentionally does not use Redis Cluster, Redis replication, Streams, or
// server-wide key discovery. Monotonic control-plane transitions are concentrated in
// small Lua scripts while cached object bodies are written with ordinary Redis commands.
package nqc
