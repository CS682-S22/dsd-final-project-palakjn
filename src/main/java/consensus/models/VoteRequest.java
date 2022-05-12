package consensus.models;

import com.google.gson.annotations.Expose;

/**
 * Responsible for holding the values required to the other node to get their vote.
 *
 * @author Palak Jain
 */
public class VoteRequest {
    @Expose
    private int cId; //Candidate ID
    @Expose
    private int cTerm; //Candidate Term
    @Expose
    private int cLogLength; //Candidate log length
    @Expose
    private int cLogTerm; //Candidate last logged term

    public VoteRequest(int cId, int cTerm, int cLogLength, int cLogTerm) {
        this.cId = cId;
        this.cTerm = cTerm;
        this.cLogLength = cLogLength;
        this.cLogTerm = cLogTerm;
    }

    /**
     * Get the id of the candidate who has sent the request to ask for vote
     */
    public int getCandidateId() {
        return cId;
    }

    /**
     * Get the candidate the term
     */
    public int getCandidateTerm() {
        return cTerm;
    }

    /**
     * Get the length of the log - Candidate has
     */
    public int getCandidateLogLength() {
        return cLogLength;
    }

    /**
     * Get the term of the last log written by the candidate
     */
    public int getCandidateLogTerm() {
        return cLogTerm;
    }
}
