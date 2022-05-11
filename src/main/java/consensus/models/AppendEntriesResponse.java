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

    public AppendEntriesResponse(int term, int ack) {
        this.term = term;
        this.ack = ack;
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
}
