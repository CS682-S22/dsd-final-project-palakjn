package consensus.models;

import com.google.gson.annotations.Expose;

import java.util.List;

/**
 * Holds all the information to be passed as part of REPLICA to the follower.
 *
 * @author Palak Jain
 */
public class AppendEntries {
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
    public List<byte[]> suffix;

    public AppendEntries(int leaderId, int term, int prefixLen, int prefixTerm, int commitLength, List<byte[]> suffix) {
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
     * Set the id of the leader
     */
    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    /**
     * Get the current term
     */
    public int getTerm() {
        return term;
    }

    /**
     * Set the current term
     */
    public void setTerm(int term) {
        this.term = term;
    }

    /**
     * Get the length of the logs' leader think - follower received
     */
    public int getPrefixLen() {
        return prefixLen;
    }

    /**
     * Set the length of the logs' leader think - follower received
     */
    public void setPrefixLen(int prefixLen) {
        this.prefixLen = prefixLen;
    }

    /**
     * Get the term of the log leader think - follower received
     */
    public int getPrefixTerm() {
        return prefixTerm;
    }

    /**
     * Set the term of the log leader think - follower received
     */
    public void setPrefixTerm(int prefixTerm) {
        this.prefixTerm = prefixTerm;
    }

    /**
     * Get the length of the logs' being committed by the leader
     */
    public int getCommitLength() {
        return commitLength;
    }

    /**
     * Set the length of the logs' being committed by the leader
     */
    public void setCommitLength(int commitLength) {
        this.commitLength = commitLength;
    }

    /**
     * Get the log at the given index
     */
    public byte[] getSuffix(int index) {
        byte[] data = null;

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
