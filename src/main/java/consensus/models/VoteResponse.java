package consensus.models;

import com.google.gson.annotations.Expose;

/**
 * Responsible for holding vote result from other peer.
 *
 * @author Palak Jain
 */
public class VoteResponse {
    @Expose
    private int nodeId;
    @Expose
    private int term;
    @Expose
    private boolean isSuccess;

    public VoteResponse(int nodeId, int term, boolean isSuccess) {
        this.nodeId = nodeId;
        this.term = term;
        this.isSuccess = isSuccess;
    }

    /**
     * Get the id of the peer who has given the vote
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * Get the term at the node side
     */
    public int getTerm() {
        return term;
    }

    /**
     * IS the vote granted or rejected
     */
    public boolean isSuccess() {
        return isSuccess;
    }
}
