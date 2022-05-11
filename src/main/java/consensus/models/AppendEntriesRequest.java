package consensus.models;

import com.google.gson.annotations.Expose;

import java.util.List;

/**
 * Holds all the information to be passed as part of REPLICA data to the follower.
 *
 * @author Palak Jain
 */
public class AppendEntriesRequest {
    @Expose
    public int leaderId;
    @Expose
    public int term;
    @Expose
    public int prefixLen;
    @Expose
    public int prefixTerm;
    @Expose
    public int commitLength;
    @Expose
    public List<Entry> suffix;

    public AppendEntriesRequest(int leaderId, int term, int prefixLen, int prefixTerm, int commitLength, List<Entry> suffix) {
        this.leaderId = leaderId;
        this.term = term;
        this.prefixLen = prefixLen;
        this.prefixTerm = prefixTerm;
        this.commitLength = commitLength;
        this.suffix = suffix;
    }

    /**
     * Get the id of the leader
     */
    public int getLeaderId() {
        return leaderId;
    }

    /**
     * Get the current term
     */
    public int getTerm() {
        return term;
    }

    /**
     * Get the length of the logs' leader think - follower received
     */
    public int getPrefixLen() {
        return prefixLen;
    }

    /**
     * Get the term of the log leader think - follower received
     */
    public int getPrefixTerm() {
        return prefixTerm;
    }

    /**
     * Get the length of the logs' being committed by the leader
     */
    public int getCommitLength() {
        return commitLength;
    }

    /**
     * Get the log at the given index
     */
    public Entry getSuffix(int index) {
        Entry data = null;

        if (index < suffix.size()) {
            data = suffix.get(index);
        }

        return data;
    }

    /**
     * Get the length of logs
     */
    public int getLength() {
        return suffix.size();
    }
}
