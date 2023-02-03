# mit_6.824_distributed_systems
This repo records my implementation for all lab assignments of the public course Mit 6.824 (http://nil.csail.mit.edu/6.824/2020/schedule.html). Thanks to MIT and professor Robert Morris for releasing this amazing course to be public!


## Content
- [x] Lab 1: Distributed MapReduce with a single master server and multiple worker servers
  - [x] implemented fault tolerance mechanism for worker crash.
- [x] Lab 2: Raft consensus algorithm implementation
  - [x] Lab 2A: Raft leader election:
    - Used a heartbeat mechanism to trigger leader election.
    - Used terms to ensure uniqueness of leader.
  - [x] Lab 2B: Raft log replication
    - Used terms and log position/index to ensure the logs up-to-date and consistency.
    - kept tracking of log index committed to state machine (client application) for each server. Indexes marked as a commitIndex are consistent for majority and safe to apply to state machine.
  - [x] Lab 2C: Raft persistent state
    - Used persistent state to recover state from machine crash.
    - More implementation to handle network delay.
  - [x] Lab 2D: Raft snapshotting/log compaction
    - Implemented snapshot mechanisms to avoid machine running out of memory and storage, including snapshot function called by server and an install snapshot RPC function used by leader to handle follower lagging too behind.
  
- [x] Lab 3: Fault-tolerant Key/Value Service
  - [x] Lab 3A: Key/value Service Without Log Compaction
    - Built a fault-tolerant key/value storage service using the Raft library in Lab 2. The server is able to handle 3 types of commands from client: "Get", "Put" and "Append", with strong consistency and linearizability from clients' perspective.
  - [x] Lab 3B: Key/value Service With Log Compaction
    - Enabled snapshot and persistent states as implemented in Raft to provide a long-running server with fault-tolerant to server crashes.

- [x] Lab 4: Multi raft for Sharded Key/Value Service 

## Environment

- Go 1.19.3

## How to run test for the system correctness
To eliminate any non-deterministic factors that possibly result in unstable test results by chance or any corner cases happenning extremely infrequently, I created a shell script that could run the tests n times and summarize the frequency of test passing; where parameter n can be manually passed in e.g. -n 50, and parameter t can be passed to assign a specific test e.g. -n 50 -t 2D (run test 2D 50 times). All the tests were run at least 50 times to ensure the system correctness.

```shell
cd mit_6.824_distributed_system
sh src/raft/runRaftTest.sh -n 50 -t 2D
```
The results would be like:
```markdown

...

starting iteration 49: 
Test (2D): snapshots basic ...
  ... Passed --   3.8  3  288  111343  224
Test (2D): install snapshots (disconnect) ...
  ... Passed --  45.2  3 1516  703411  328
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  60.8  3 1906  776713  319
Test (2D): install snapshots (crash) ...
  ... Passed --  27.1  3  876  391973  290
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  31.3  3 1024  546287  361
Test (2D): crash and restart all servers ...
  ... Passed --   7.1  3  246   72604   52
PASS
ok  	6.824/raft	175.682s

starting iteration 50: 
Test (2D): snapshots basic ...
  ... Passed --   3.8  3  276  106947  207
Test (2D): install snapshots (disconnect) ...
  ... Passed --  45.8  3 1494  644012  319
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  61.9  3 1918  810739  320
Test (2D): install snapshots (crash) ...
  ... Passed --  26.9  3  906  463390  335
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  36.9  3 1142  561211  325
Test (2D): crash and restart all servers ...
  ... Passed --   8.3  3  302   89852   65
PASS
ok  	6.824/raft	184.066s

Running the test a total of 50 times, with 50 times passed. 
Total time spent: 11045.9 seconds.
```
