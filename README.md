# mit_6.824_distributed_systems
This repo records my implementation for all lab assignments of the public course Mit 6.824 (http://nil.csail.mit.edu/6.824/2021/schedule.html). Thanks to MIT and professor Robert Morris for releasing this amazing course to be public!


## Content
- [x] Lab 1: Distributed MapReduce with a single master server and multiple worker servers
  - [x] implemented fault tolerant for worker crash.
- [x] Lab 2: Raft consensus algorithm implementation
  - [x] Lab 2A: Raft leader election:
    - Used a heartbeat mechanism to trigger leader election.
    - Used terms to ensure uniqueness of leader.
  - [x] Lab 2B: Raft log replication
    - Used terms and log position/index to ensure the logs up-to-date and consistency.
    - kept tracking of log index committed to state machine (client application) for each server. Indexes marked as a commitIndex are consistent for majority and safe to apply to state machine.
  - [x] Lab 2C: Raft persistent state
  - [ ] Lab 2D: Raft snapshotting/log compaction
  
- [ ] Lab 3: Fault-tolerant Key/Value Service
  - [ ] Lab 3A: Key/value Service Without Log Compaction
  - [ ] Lab 3B: Key/value Service With Log Compaction

- [ ] Lab 4: Sharded Key/Value Service

## Environment

- Go 1.19.3

## How to Run Test
To eliminate any non-deterministic factors that possibly result in unstable test results by chance or any corner cases happenning extremely infrequently, I created a shell script that could run the tests n times and summarize the frequency of test passing; where n can be manually passed in e.g. -n 100.

```shell
cd mit_6.824_distributed_system
sh src/raft/runRaftTest.sh -n 100
```
