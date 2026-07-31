The Firestore long-term memory backend now stores keys containing `/` (for example `case/2`).
Previously any hierarchical key was rejected as an invalid Firestore document reference, so the
Firestore backend could not serve workloads the Redis and Bigtable backends handled.
