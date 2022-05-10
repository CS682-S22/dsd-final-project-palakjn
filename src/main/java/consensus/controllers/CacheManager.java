package consensus.controllers;

import configuration.Constants;
import models.Host;
import consensus.controllers.database.EntryDB;
import consensus.models.Entry;
import consensus.models.NodeState;
import utils.FileManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Static global cache alike class storing state, offsets, term information in-memory.
 *
 * @author Palak Jain
 */
public class CacheManager {
    private static Members members = new Members();
    private static NodeState nodeState = new NodeState();
    private static List<Integer> sentLength = new ArrayList<>(); //Total number of the logs being sent to other nodes. Log index represents node id.
    private static List<Integer> ackedLength = new ArrayList<>(); // Total number of the logs received by the other nodes. Log index represents node id.
    private static List<Integer>  lastLogTermReceived = new ArrayList<>(); //Term of the last log received by the other nodes. Log index represents node id.
    private static List<Entry> entries = new ArrayList<>(); //List of offsets of the received log from client/leader

    //Locks
    private static ReentrantReadWriteLock memberLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock statusLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock dataLock = new ReentrantReadWriteLock();

    /**
     * Set the details of server running the current instance
     */
    public static void setLocal(Host local) {
        memberLock.writeLock().lock();
        members.setLocal(local);
        memberLock.writeLock().unlock();
    }

    /**
     * Get the details of server running the current instance
     */
    public static Host getLocal() {
        memberLock.readLock().lock();

        try {
            return members.getLocalHost();
        } finally {
            memberLock.readLock().unlock();
        }
    }

    /**
     * Add new member if not exist
     */
    public static void addMember(Host member) {
        memberLock.writeLock().lock();
        members.addMember(member);
        memberLock.writeLock().unlock();
    }

    /**
     * Get the member with the given id
     */
    public static Host getMember(int id) {
        memberLock.readLock().lock();

        try {
            return members.getMember(id);
        } finally {
            memberLock.readLock().unlock();
        }
    }

    /**
     * Get all the other members in the zone
     */
    public static List<Host> getNeighbors() {
        memberLock.readLock().lock();

        try {
            return members.getNeighbors();
        } finally {
            memberLock.readLock().unlock();
        }
    }

    /**
     * Set the status of the node
     */
    public static void setNodeState(NodeState nodeState) {
        statusLock.writeLock().lock();
        CacheManager.nodeState = nodeState;
        statusLock.writeLock().unlock();
    }

    /**
     * Get the current term
     */
    public static int getTerm() {
        statusLock.readLock().lock();

        try {
            return nodeState.getTerm();
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Set the current term with the given value
     */
    public static void setTerm(int term) {
        statusLock.writeLock().lock();

        nodeState.setTerm(term);

        statusLock.writeLock().unlock();
    }

    /**
     * Increment the current term by one
     */
    public static void incrementTerm() {
        statusLock.writeLock().lock();

        nodeState.incrementTerm();

        statusLock.writeLock().unlock();
    }

    /**
     * Get the id of the candidate - the follower voted for the current term
     */
    public static int getVotedFor() {
        statusLock.readLock().lock();

        try {
            return nodeState.getVotedFor();
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Checking whether the follower voted for the given node id
     */
    public static boolean isAlreadyVotedFor(int nodeId) {
        statusLock.readLock().lock();

        try {
            return nodeState.isAlreadyVotedFor(nodeId);
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Set the candidate id for whom the follower given the vote for the given term
     */
    public static void setVoteFor(int nodeId) {
        statusLock.writeLock().lock();
        nodeState.setVoteFor(nodeId);
        statusLock.writeLock().unlock();
    }

    /**
     * Get the number of the logs being committed/sent to the state machine
     */
    public static int getCommitLength() {
        dataLock.readLock().lock();

        try {
            return nodeState.getCommitLength();
        } finally {
            dataLock.readLock().unlock();
        }
    }

    /**
     * Set the number of the logs being committed/sent to the state machine
     */
    public static void setCommitLength(int commitLength) {
        dataLock.writeLock().lock();
        nodeState.setCommitLength(commitLength);
        dataLock.writeLock().unlock();
    }

    /**
     * Get the current role of the server (Candidate/Leader/Follower)
     */
    public static int getCurrentRole() {
        statusLock.readLock().lock();

        try {
            return nodeState.getCurrentRole();
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Set the current role of the server (Candidate/Leader/Follower)
     */
    public static void setCurrentRole(int currentRole) {
        statusLock.writeLock().lock();

        nodeState.setCurrentRole(currentRole);

        statusLock.writeLock().unlock();
    }

    /**
     * Get the id of the current leader
     */
    public static int getCurrentLeader() {
        statusLock.readLock().lock();

        try {
            return nodeState.getCurrentLeader();
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Set the id of the current leader
     */
    public static void setCurrentLeader(int currentLeader) {
        statusLock.writeLock().lock();

        nodeState.setCurrentLeader(currentLeader);

        statusLock.writeLock().unlock();
    }

    /**
     * Get the last received starting offset from the given client
     */
    public static int getLastReceivedOffset(String clientId) {
        dataLock.readLock().lock();
        int clientOffset = -1;
        int lastIndex = entries.size() - 1;

        while (lastIndex >= 0) {
            Entry entry = entries.get(lastIndex);

            if (entry.getClientId().equals(clientId)) {
                clientOffset = entry.getReceivedOffset();
                break;
            }

            lastIndex--;
        }

        dataLock.readLock().unlock();
        return clientOffset;
    }

    /**
     * Set the number of logs being known to be sent to the given nodeId
     */
    public static void setSentLength(int nodeId, int length) {
        dataLock.writeLock().lock();

        sentLength.set(nodeId, length);

        dataLock.writeLock().unlock();
    }

    /**
     * Get the number of logs being known to be sent to the given nodeId
     */
    public static int getSentLength(int nodeId) {
        dataLock.readLock().lock();

        try {
            return sentLength.get(nodeId);
        } finally {
            dataLock.readLock().unlock();
        }
    }

    /**
     * Set the number of logs being received by the given nodeId
     */
    public static void setAckedLength(int nodeId, int length) {
        dataLock.writeLock().lock();
        ackedLength.add(nodeId, length);
        dataLock.writeLock().unlock();
    }

    /**
     * Get the number of logs being received by the given nodeId
     */
    public static int getAckedLength(int nodeId) {
        dataLock.readLock().lock();

        try {
            return ackedLength.get(nodeId);
        } finally {
            dataLock.readLock().unlock();
        }
    }

    /**
     * Get the term of the last message being written by the given nodeId
     */
    public static int getLastLogTermAck(int nodeId) {
        dataLock.readLock().lock();

        try {
            return lastLogTermReceived.get(nodeId);
        } finally {
            dataLock.readLock().unlock();
        }
    }

    /**
     * Set the term of the last message being written by the given nodeId
     */
    public static void setLastLogTermAck(int nodeId, int term) {
        dataLock.writeLock().lock();

        lastLogTermReceived.set(nodeId, term);

        dataLock.writeLock().unlock();
    }

    /**
     * Append log to the local file and add term, offset information as new entry to in-memory and db
     */
    public static boolean addEntry(byte[] log, String clientId, int clientOffset) {
        dataLock.writeLock().lock();
        //Writing log data to local file
        int offset = 0;
        if (entries.size() > 0) {
            offset = entries.get(entries.size() - 1).getToOffset() + 1;
        }
        boolean isSuccess = FileManager.write(log, offset);

        if (isSuccess) {
            //Adding new entry to in-memory data structure
            Entry entry = new Entry(getTerm(), offset, offset + log.length - 1, clientId, clientOffset);
            entries.add(entry);

            //Adding new entry to SQL
            EntryDB.insert(entry);
        }

        dataLock.writeLock().unlock();
        return isSuccess;
    }

    /**
     * Add the entry to the collection
     */
    public static void addEntry(Entry entry) {
        dataLock.writeLock().lock();
        entries.add(entry);
        dataLock.writeLock().unlock();
    }

    /**
     * Get the length of received log
     */
    public static int getLogLength() {
        dataLock.readLock().lock();

        try {
            return entries.size();
        } finally {
            dataLock.readLock().unlock();
        }
    }

    /**
     * Get BATCH-SIZE number of entries from the given index
     */
    public static List<Entry> getEntries(int startIndex) {
        dataLock.readLock().lock();
        List<Entry> entries = new ArrayList<>();
        int logLength = CacheManager.entries.size();
        int count = 0;

        while (startIndex < logLength && count < Constants.SUFFIX_BATCH_SIZE) {
            entries.add(CacheManager.entries.get(startIndex));

            startIndex++;
            count++;
        }

        dataLock.readLock().unlock();
        return entries;
    }

    /**
     * Get the entry at the given index
     */
    public static Entry getEntry(int index) {
        dataLock.readLock().lock();
        Entry entry = null;

        if (index < entries.size()) {
            entry = entries.get(index);
        }

        return entry;
    }

    /**
     * Remove the entries from the given index from in-memory and db
     */
    public static void removeEntryFrom(int index) {
        dataLock.writeLock().lock();

        //TODO: Get the fromOffset from the entry at the given index

        entries.subList(index, entries.size() - 1).clear();

        //TODO: Removing these entries from SQL table

        dataLock.writeLock().unlock();
    }
}
