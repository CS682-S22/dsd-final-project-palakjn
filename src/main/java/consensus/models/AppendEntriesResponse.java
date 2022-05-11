package consensus.models;

import com.google.gson.annotations.Expose;

/**
 * Holds all the information passed by the follower as a response to the AppendEntry packet
 *
 * @author Palak Jain
 */
public class AppendEntriesResponse {
    @Expose
    private int term;
    @Expose
    private int ack;
    @Expose
    private int nodeId;
    @Expose
    private boolean isSuccess;

    public AppendEntriesResponse(int term, int ack, int nodeId, boolean isSuccess) {
        this.term = term;
        this.ack = ack;
        this.nodeId = nodeId;
        this.isSuccess = isSuccess;
    }

    /**
     * Get the term at the follower side
     */
    public int getTerm() {
        return term;
    }

    /**
     * Get the number of logs being received by the follower
     */
    public int getAck() {
        return ack;
    }

    /**
     * Get the id of the node responding
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * Set the id of the node responding
     */
    public boolean isSuccess() {
        return isSuccess;
    }
}
