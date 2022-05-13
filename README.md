# Implementing Distributed publisher subscriber System by using Raft Consensus Algorithm
***

> **Note:** The application has been taken only for learning how Raft Consensus Algorithm works, it may not exactly mimic the real distributed banking system.

### Description

* Building leader based distributed system comprising n static nodes.
* There can be more than one Producer and consumer.
* Producers will be publishing logs to the leader of the distributed system
* Consumer will be pulling logs from the leader of the distributed system
* Below is the architecture of the system:

&nbsp;
&nbsp;
&nbsp;

<p align="center">
  <img width="460" height="300" src="https://info.container-solutions.com/hubfs/Imported_Blog_Media/figure1_raft-1.png">
</p>

&nbsp;
&nbsp;
&nbsp;

* Broker comprises two modules - Consensus module and state machine
* Whenever producer will issue a log to a leader, it will first go to the consensus module which will ensure that the log should be written first to its local storage and then to the quorum of the nodes in the system.
* Once the log is being committed in the majority of the nodes then only the log will be sent to the state machine.
* State machine will write the log to the log location which contains all the committed logs and will acknowledge back to the producer.
* Consumer will periodically pull logs from the broker with the batch size of 10.
* Consumer can ask the broker to send logs from specific offset

***

### Characteristics

**1) Initial Phase**

* Starting with fixed configuration for a cluster.
* The broker will have n nodes (pre-configured) and each node will know about other n-1 nodes.
* Initially, each broker will join the network as follower. Leader will be chosen after election. 

**2) System Model**

* Supporting crash recovery model.
* Will use local disk to keep the status like current term, state, logs, etc. in order to support crash recovery model.

**3) Strong Consistency**

* Consumer will get the logs in the same order as the system received it from the producer.

**4) Fault Tolerance**

* Making system fault-tolerant by replicating all the logs' stored by the leader to n other node in the system.
* The system will tolerate less than half of n nodes.

**5) Fault Detection and Election Process**

* Detecting the failure of leader by using Raft mechanism where follower will deduct the failure of the leader if it hasn't received the heartbeat message (AppendEntries) from the leader withing the certain amount of time.
* Choosing leader by collecting the majority of votes from other nodes.

**6) No Duplicates**

* System will ensure not to append same log as received before from the producer


***

### Language

* Will use Java
* Will use ProtocolBuffer plus JSON for exchanging data between nodes.

***

### Additional Packages

* Protobuf
* GSON
* log4j
* MySQL 


