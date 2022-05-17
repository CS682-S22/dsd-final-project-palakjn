package consensus.controllers;

import consensus.controllers.database.EntryDB;
import consensus.controllers.database.StateDB;
import consensus.models.Entry;
import consensus.models.NodeState;
import models.Host;
import utils.FileManager;
import utils.Strings;

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
    public static Host getNeighbor(int id) {
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
     * Remove all members
     */
    public static void removeMembers() {
        memberLock.writeLock().lock();

        members.clear();

        memberLock.writeLock().unlock();
    }

    /**
     * Get all the members
     */
    public static List<Host> getMembers() {
        memberLock.readLock().lock();

        try {
            return members.getMembers();
        } finally {
            memberLock.readLock().unlock();
        }
    }

    /**
     * Get the number of members in the system
     */
    public static int getMemberCount() {
        memberLock.readLock().lock();
        int count = 1; //Counting for itself

        count += members.getCount();

        memberLock.readLock().unlock();
        return count;
    }

    /**
     * Set the status of the node
     */
    public static void setNodeState(NodeState nodeState) {
        statusLock.writeLock().lock();
        CacheManager.nodeState = new NodeState(nodeState);

        StateDB.upsert(CacheManager.nodeState);
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
    public static int setTerm(int term) {
        statusLock.writeLock().lock();

        nodeState.setTerm(term);

        try {
            StateDB.upsert(CacheManager.nodeState);
            return nodeState.getTerm();
        } finally {
            statusLock.writeLock().unlock();
        }
    }

    /**
     * Get the location of the file which contains commit logs
     */
    public static String getCommitLocation() {
        statusLock.readLock().lock();

        try {
            return nodeState.getCommitLocation();
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Get the location of the file which contains logs written by consensus system
     */
    public static String getLocation() {
        statusLock.readLock().lock();

        try {
            return nodeState.getLocation();
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Set the location of the file which contains commit logs
     */
    public static void setCommitLocation(String location) {
        statusLock.writeLock().lock();
        nodeState.setCommitLocation(location);
        StateDB.upsert(CacheManager.nodeState);
        statusLock.writeLock().unlock();
    }

    /**
     * Set the location of the file which contains logs written by consensus system
     */
    public static void setLocation(String location) {
        statusLock.writeLock().lock();
        nodeState.setLocation(location);
        StateDB.upsert(CacheManager.nodeState);
        statusLock.writeLock().unlock();
    }

    /**
     * Increment the current term by one
     */
    public static int incrementTerm() {
        statusLock.writeLock().lock();

        nodeState.incrementTerm();
        StateDB.upsert(CacheManager.nodeState);

        statusLock.writeLock().unlock();
        return nodeState.getTerm();
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
     * Set the candidate id for whom the follower given the vote for the given term
     */
    public static boolean setVoteFor(int nodeId, boolean force) {
        statusLock.writeLock().lock();

        boolean isVoted = false;

        if (nodeId == -1) {
            nodeState.setVoteFor(-1);
        } else if (force || nodeState.getVotedFor() == -1 || nodeState.getVotedFor() != nodeId) {
            nodeState.setVoteFor(nodeId);
            isVoted = true;
        }

        StateDB.upsert(CacheManager.nodeState);
        statusLock.writeLock().unlock();
        return isVoted;
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
        StateDB.upsert(CacheManager.nodeState);
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
        StateDB.upsert(CacheManager.nodeState);

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
        StateDB.upsert(CacheManager.nodeState);

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

            if (!Strings.isNullOrEmpty(entry.getClientId()) && entry.getReceivedOffset() != -1 && entry.getClientId().equals(clientId)) {
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
     * Initialize sentLength and ackLength with 0 for the given number of entries
     */
    public static void initSentAndAckLength() {
        dataLock.writeLock().lock();

        int count = getMemberCount();

        for (int index = 0; index < count; index++) {
            sentLength.add(0);
            ackedLength.add(0);
        }

        dataLock.writeLock().unlock();
    }

    /**
     * Decrement sendLength of the given node by 1
     */
    public static void decrementSentLength(int nodeId) {
        dataLock.writeLock().lock();

        int length = sentLength.get(nodeId);
        sentLength.set(nodeId, length - 1);

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
     * Append log to the local file and add term, offset information as new entry to in-memory and db
     */
    public static boolean addEntry(byte[] log, int term, String clientId, int clientOffset) {
        dataLock.writeLock().lock();
        //Writing log data to local file
        int offset = getLastOffset(false);
        boolean isSuccess = FileManager.write(CacheManager.getLocation(), log, offset);

        if (isSuccess) {
            //Adding new entry to in-memory data structure
            Entry entry = new Entry(term, offset, offset + log.length - 1, clientId, clientOffset, false);
            entries.add(entry);

            //Adding new entry to SQL
            EntryDB.insert(entry);
        }

        dataLock.writeLock().unlock();
        return isSuccess;
    }

    /**
     * Get the offset from where to write to the file
     */
    public static int getLastOffset(boolean commit) {
        dataLock.readLock().lock();
        int offset = 0;

        if (entries.size() > 0) {
            for (int index = entries.size() - 1; index >= 0; index--) {
                if (!commit || entries.get(index).isCommitted()) {
                    offset = entries.get(index).getToOffset() + 1;
                    break;
                }
            }
        }

        dataLock.readLock().unlock();
        return offset;
    }

    /**
     * Get the index of the entry
     */
    public static int getEntryIndex(int fromOffset, boolean commitOnly) {
        dataLock.readLock().lock();
        int entryIndex = -1;

        for (int index = 0; index < entries.size(); index++) {
            Entry entry = entries.get(index);

            if (entry.getFromOffset() == fromOffset) {
                if (!commitOnly || entry.isCommitted()) {
                    entryIndex = index;
                }

                break;
            }
        }

        dataLock.readLock().unlock();
        return entryIndex;
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
     * If commitOnly is true then, send given number of entries from the given index only if they are committed
     * If commitOnly is false then, send given number of entries from the given index
     */
    public static List<Entry> getEntries(int startIndex, int batchCount, boolean commitOnly) {
        dataLock.readLock().lock();
        List<Entry> entries = new ArrayList<>();
        int logLength = CacheManager.entries.size();
        int count = 0;

        while (startIndex < logLength && count < batchCount) {
            Entry entry = CacheManager.entries.get(startIndex);
            if (commitOnly) {
                if (entry.isCommitted()) {
                    entries.add(new Entry(entry));
                } else {
                    break;
                }
            } else {
                entries.add(new Entry(entry));
            }

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

        dataLock.readLock().unlock();
        return entry;
    }

    /**
     * Getting the last log term
     */
    public static int getLastTerm() {
        dataLock.readLock().lock();
        int term = 0;

        if (entries.size() > 0) {
            term = entries.get(entries.size() - 1).getTerm();
        }

        dataLock.readLock().unlock();
        return term;
    }

    /**
     * Remove the entries from the given index from in-memory and db
     */
    public static void removeEntryFrom(int index) {
        dataLock.writeLock().lock();

        Entry entry = entries.get(index);
        int fromOffset = entry.getFromOffset();

        List<Entry> temp = new ArrayList<>();

        for (int i = 0; i < index; i++) {
            temp.add(entries.get(i));
        }

        entries = temp;
        EntryDB.deleteFrom(fromOffset);

        dataLock.writeLock().unlock();
    }
}
