# logma-nqc

"Not Quite Consistency" (NQC): a design for using independent, uncoordinated standalone
Redis OSS instances (no Cluster, no Stack, no replication) as CDN cache islands, kept
roughly in sync by Redis Pub/Sub hints plus periodic anti-entropy reconciliation against
an authoritative origin.

This repo currently holds the design doc only — see [`docs/nqc-design.md`](docs/nqc-design.md)
for the full protocol, its consistency contract, robustness analysis, and illustrative Go
interfaces. Nothing here is implemented yet.
