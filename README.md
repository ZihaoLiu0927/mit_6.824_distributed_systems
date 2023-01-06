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
  - [ ] Lab 3B: Key/value Service With Log Compaction

## Environment

- Go 1.19.3

## How to Run Test
To eliminate any non-deterministic factors that possibly result in unstable test results by chance or any corner cases happenning extremely infrequently, I created a shell script that could run the tests n times and summarize the frequency of test passing; where parameter n can be manually passed in e.g. -n 100, and parameter t can be passed to assign a specific test e.g. -n 2D (run test 2D 100 times).

```shell
cd mit_6.824_distributed_system
sh src/raft/runRaftTest.sh -n 20 -t 2D
```
The results would be like:
```markdown

...

starting iteration 19: 
Test (2D): snapshots basic ...
  ... Passed --   3.9  3  226   89828  187
Test (2D): install snapshots (disconnect) ...
  ... Passed --  46.2  3 1534  678760  345
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  61.3  3 1910  890935  336
Test (2D): install snapshots (crash) ...
  ... Passed --  26.7  3  882  482514  337
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  32.8  3 1010  535288  336
Test (2D): crash and restart all servers ...
  ... Passed --   7.7  3  274   81210   58
PASS
ok  	6.824/raft	179.346s

starting iteration 20: 
Test (2D): snapshots basic ...
  ... Passed --   3.9  3  276  110691  209
Test (2D): install snapshots (disconnect) ...
  ... Passed --  45.0  3 1472  691877  298
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  64.1  3 1966  832868  329
Test (2D): install snapshots (crash) ...
  ... Passed --  27.5  3  874  413997  294
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  34.8  3 1068  541261  317
Test (2D): crash and restart all servers ...
  ... Passed --   8.6  3  318   94702   70
PASS
ok  	6.824/raft	184.579s

Test Progress: [####################]100.0%

Running the test a total of 20 times, with 20 times passed. 
Total time spent: 3599.9 seconds.
```
