# Report.md

Our implementation is inspired by Chord and Harp, but we also made some modifications.
In essense, we arranged the backends and keepers into two rings with keepers in the inner ring and backends in the outer ring. Then we cut the circles into (roughly) equal sectors with one keeper and multiple backends in each sector (thanks to Zac for the illustrative example and idea).

## Keepers

Keepers are responsible for a (roughly) equal subset of backends based on how many keepers are alive. To do this, we created some protos and RPC calls that allow keepers to talk to each other. Keepers regularly check the liveness of other keepers by sending RPC requests. Those requests are to fetch backend status from each keeper as to maintain a consistent view of the status of each backend. In the case of keepers joining/leaving, the live keepers adjust their responsible backends by rearranging their sector range.

The keepers also handle events when the backend servers join or leave. A backend who joins the system must be alive and it was previously marked as dead in the backend status list. Then if so, the responsible keeper of that backend fetchs data from the successor backend on the ring to update the joined backend's storage. In the case of a node leaving event, the responsible keeper makes necessary replications between the predecessor and three successors of the leaving backend.

The keeper's regular check procedure is as follows (triggered every 1 second):
- Check liveness of all keepers and collect backend status from them
- Update backend status that falls in the keeper's controlling range
- Adjust the set of controlled backends and contact them for syncing
- Synchronize controlled backend clocks and do necessary replication

## BinStorage Client

We had to modify the bin storage client to support multiple backend storage replicas. For set-type requests, we want to guarantee that we've written to up to 3 backends available. The data persists in the system as long as write to one backend succeed, assuming there's always at least one backend online.

For other get-type requests, it is possible that a front may request some data from a backend that is currently in the state of migration. Thus, the backend may not have the most up-to-date data but there must be a copy on the migrating backend's successor(s). We ended up querying up to 3 backends from the one we are supposed to retrieve data from (ie. hashed to) and returning the most recent data among all retrieved from those backends.

## Scalability Analysis

- During each keeper check iteration, only `n^2` RPC calls are made between keepers and, at worst case, `O(m)` TCP connections attempts (`n` is the number of keepers and `m` is the number of backends). TCP connections, once established, is cached for future use.
- In the event of keeper/backend joins/leaves, the system stabalizes in at most `2*(keeper check interval)` time.
- When replication happens, at most 3 servers are copied and dumped to other servers. Since those replications are concurrent, the whole step always take roughly the same time as doing one whole server replication.