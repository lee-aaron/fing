Data Replication:
1. A backend shares data with 2 other successor live nodes.
2. The backend index for a key is determined by a hash function. The next live node at or after the index has the key.
3. A backend's uccessors are the 2 live nodes after its index (with wrap around).
4. When a backend crashes, its immediate successor becomes the "primary" responsible for the data (ie. assume O->A->B->C and A crashes).
5. O shares data with B and C, and C shares data with its successor.
6. When a backend joins (ie. assume X joins between O and B), it's enough to have only B share data with X as coordinated by the keeper.

Keeper Responsibility:
1. Keeps connections to every other keeper and checks the liveness of them at every heartbeat.
2. Also keeps connecctions to every backends and checks liveness at every heartbeat for potential replication processes.
3. If a keeper crashes, other keepers re-adjust themselves to take responsibility of their allocated nodes (ie. nodes in the range: [nodes #/keeper #\*keeper id, nodes #/keeper #\*(keeper id+1)]; backends that got left over go to the last keeper).
4. If a keeper joins, the same adjustment happens and it must establish connections with other keepers.
5. In every heartbeat, keeper checks the liveness of other keepers as above and liveness of their "ruled" backends.
6. If a backend crashes or joins, share data as described before (may transfer data to backend "ruled" by another keeper).
7. **Edge case**: If a keeper K is originally responsible for a backend A and it crashes. During the next heartbeat, K realizes A is now the responsibility of a newly joined keeper K'. Before handing over the responsibility, it must replicate data as described before since K' may not be aware of A crashing.

Clock Synchronization:
1. Different keepers may have different clocks during their "rulings" of the backends.
2. (Does this work?) When a keeper changes its "ruling" set of backends, it readjust its clock to the max clock of all the backends and bring all other backends' clocks to date.
3. When a keeper joins, it will first attempt to contact to all keepers, and start serving its portion of backends immediately.
4. It's possible during this time, 2 keepers are syncing the clock on one backend (ie. the prev keeper hasn't send out heartbeats yet). This is fine since clock on backends only increments (ie. only increments more than necessary in this case).