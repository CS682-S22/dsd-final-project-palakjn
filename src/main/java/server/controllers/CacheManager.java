package server.controllers;

import server.models.Entry;
import server.models.NodeState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Static global cache alike class storing state, offsets, term information in-memory.
 *
 * @author Palak Jain
 */
public class CacheManager {
    private static NodeState nodeState = new NodeState();
    private static List<Integer> sentLength = new ArrayList<>(); //Total number of the logs being sent to other nodes. Log index represents node id.
    private static List<Integer> ackedLength = new ArrayList<>(); // Total number of the logs received by the other nodes. Log index represents node id.
    private static List<Integer>  lastLogTermReceived = new ArrayList<>(); //Term of the last log received by the other nodes. Log index represents node id.
    private static List<Entry> entries = new ArrayList<>(); //List of offsets of the received log from client/leader

    //Locks
    private static ReentrantReadWriteLock statusLock = new ReentrantReadWriteLock();
    private static ReentrantReadWriteLock dataLock = new ReentrantReadWriteLock();

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

        ackedLength.set(nodeId, length);

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
    public static void addEntry(int term, byte[] log) {
        dataLock.writeLock().lock();
        //TODO: Write to local file

        //TODO: entries.add(entry);

        //TODO: Insert entry MD to SQL

        dataLock.writeLock().unlock();
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
