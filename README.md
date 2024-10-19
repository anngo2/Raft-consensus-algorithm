# Raft-consensus-algorithm

Raft is a distributed consensus algorithm. Raft.go is the full implementation of <a href="https://raft.github.io/raft.pdf">Raft</a>. The algorithm effectively selects leaders through timed elections and voting. It is able to handle all of the failure senarios listed in the paper using log replication.

The core components of the Raft algorithm includes:

**Leader Election:** When no leader is present, or the current leader fails, servers elect a new leader through a majority vote.

**Log Replication:** The leader handles log replication by sending appendEntries requests to follower servers.

**Fault Tolerance:** Raft ensures fault tolerance through state persistence, commit rules, and recovery mechanisms, allowing the system to continue operating even if servers fail.


<big>**Features**</big>

- Raft uses leader election to ensure that only one node in the cluster is responsible for coordinating client requests. The leader is selected through a voting process, and only one leader can exist at any given time.
- Once elected, the leader is responsible for replicating logs (client commands) to follower servers. It ensures that all servers apply the same operations in the same order.
- Raft provides strong consistency guarantees. Even in the event of server failures, Raft ensures that no conflicting decisions are made, and that all servers apply committed log entries in the same sequence.
- Raft ensures durability and reliability by persisting crucial information such as the current term, votes, and logs to stable storage. This allows the system to recover from crashes while maintaining data integrity.
- Raft effectively tolerates server failures as long as a majority of servers (a quorum) remain operational. If a leader crashes, a new leader is quickly elected to maintain system availability.


