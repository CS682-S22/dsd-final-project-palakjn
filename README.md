# Implementing Distributed Banking System by using Raft Consensus Algorithm
***

> **Note:** The application has been taken only for learning how Raft Consensus Algorithm works, it may not exactly mimic the real distributed banking system.

### Description

* Building leader based distributed system comprising n static nodes.
* There can be more than one client.
* The clients can have multiple accounts.
* These accounts will be distributed among multiple servers in a cluster.
* Clients can issue transactions only to the leader of the system.
* The mode of transfer of these transactions is by log.
* Each log contains the single command to execute by the system. The system accepts only those logs which have the command written in acceptable format only. 
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

* Whenever a client will issue a log to a leader, it will go to the consensus module first which will ensure that the log should be written first to its local storage and then to the quorum of the nodes in the system.
* Once the log is being committed in the majority of the nodes then only the log will be sent to the state machine.
* The state machine will parse the received log to get the command to execute. 
* The state machine will validate the command whether the command is valid or not. If it is valid, it will execute the command and will acknowledge the client. If it is not valid then, it will send negative acknowledgment to the client.

***

### Characteristics

**1) Initial Phase**

* Starting with fixed configuration for a cluster.
* The system will have n nodes (pre-configured) and each node will know about other n-1 nodes.
* Initially, each node will join the network as follower. Leader will be chosen after election. 

**2) System Model**

* Supporting crash recovery model.
* Will use local disk to keep the status like current term, state, logs, etc. in order to support crash recovery model.

**3) Strong Consistency**

* All the state machines will execute same set of commands in the exact same order as being issued by the client.

**4) Fault Tolerance**

* Making system fault-tolerant by replicating all the logs' stored by the leader to n other node in the system.
* The system will tolerate less than half of n nodes.

**5) Fault Detection and Election Process**

* Detecting the failure of leader by using Raft mechanism where follower will deduct the failure of the leader if it hasn't received the heartbeat message (AppendEntries) from the leader withing the certain amount of time.
* Choosing leader by collecting the majority of votes from other nodes.

***

### Types of commands as log line

**1) OPEN ACCOUNT <name>**

* Example: OPEN ACCOUNT Palak
* Will open the new account if not exist
* If the account with the same name already exists then will send negative acknowledgement. 

**2) CREDIT \<amount> \<name>**

* Example: CREDIT 100 Palak
* Adding the non-zero amount to the account with the given name if existed.
* Will send NACK if account with the given name not exist or the amount < 1.

**3) DEPOSIT \<amount> \<name>**

* Example: DEPOSIT 100 Palak
* If account exists with the given name and the available balance is more than the given amount then subtract the given amount from the available amount.
* Else, send NACK to the client.

**4) SHOW BALANCE <name>**

* Example: SHOW BALANCE Palak
* If the account exist with the given name then send the available balance.
* Else, send NACK to the client.

**5) CLOSE \<name>**

* Example: CLOSE Palak
* If the account exist with the given name then remove the account reference.
* Else, send NACK to the client

***

### Language

* Will use Java
* Will use ProtocolBuffer plus JSON for exchanging data between nodes.

***

### Additional Packages

* Protobuf
* GSON
* log4j
* MySQL (Still deciding on this one. Maybe I will store the necessary attributes in SQL)

***

### Optional Feature

If time permits, I will implement Joint Consensus to support configuration changes where I can crash stop few nodes or add new nodes.

