package consensus.models;

import configuration.Constants;

/**
 * Holds the information of current status of the server
 *
 * @author Palak Jain
 */
public class NodeState {
    private int term;
    private int votedFor;
    private int commitLength;
    private int currentRole;
    private int currentLeader;
    private String commitLocation;
    private String location;

    public NodeState(int term, int votedFor, int commitLength, int currentLeader, String commitLocation, String location) {
        this.term = term;
        this.votedFor = votedFor;
        this.commitLength = commitLength;
        this.currentLeader = currentLeader;
        this.commitLocation = commitLocation;
        this.location = location;
    }

    public NodeState(NodeState nodeState) {
        term = nodeState.getTerm();
        votedFor = nodeState.getVotedFor();
        commitLength = nodeState.getCommitLength();
        currentLeader = nodeState.getCurrentLeader();
        commitLocation = nodeState.getCommitLocation();
        location = nodeState.getLocation();
    }

    public NodeState() {
        this.votedFor = -1;
        this.currentRole = Constants.ROLE.FOLLOWER.ordinal();
        this.currentLeader = -1;
    }

    /**
     * Get the location of the file which contains commit logs
     */
    public String getCommitLocation() {
        return commitLocation;
    }

    /**
     * Get the location of the file which contains logs written by consensus system
     */
    public String getLocation() {
        return location;
    }

    /**
     * Set the location of the file which contains commit logs
     */
    public void setCommitLocation(String commitLocation) {
        this.commitLocation = commitLocation;
    }

    /**
     * Set the location of the file which contains logs written by consensus system
     */
    public void setLocation(String location) {
        this.location = location;
    }

    /**
     * Get the current term
     */
    public int getTerm() {
        return term;
    }

    /**
     * Set the current term with the given value
     */
    public void setTerm(int term) {
        this.term = term;
    }

    /**
     * Increment the current term by one
     */
    public void incrementTerm() {
        term++;
    }

    /**
     * Get the id of the candidate - the follower voted for the current term
     */
    public int getVotedFor() {
        return votedFor;
    }

    /**
     * Checking whether the follower voted for the given node id
     */
    public boolean isAlreadyVotedFor(int nodeId) {
        return !(votedFor == -1 || votedFor != nodeId);
    }

    /**
     * Set the candidate id for whom the follower given the vote for the given term
     */
    public void setVoteFor(int nodeId) {
        votedFor = nodeId;
    }

    /**
     * Get the number of the logs being committed/sent to the state machine
     */
    public int getCommitLength() {
        return commitLength;
    }

    /**
     * Set the number of the logs being committed/sent to the state machine
     */
    public void setCommitLength(int commitLength) {
        this.commitLength = commitLength;
    }

    /**
     * Get the current role of the server (Candidate/Leader/Follower)
     */
    public int getCurrentRole() {
        return currentRole;
    }

    /**
     * Set the current role of the server (Candidate/Leader/Follower)
     */
    public void setCurrentRole(int currentRole) {
        this.currentRole = currentRole;
    }

    /**
     * Get the id of the current leader
     */
    public int getCurrentLeader() {
        return currentLeader;
    }

    /**
     * Set the id of the current leader
     */
    public void setCurrentLeader(int currentLeader) {
        this.currentLeader = currentLeader;
    }

    @Override
    public String toString() {
        return "NodeState{" +
                "term=" + term +
                ", votedFor=" + votedFor +
                ", commitLength=" + commitLength +
                ", currentRole=" + currentRole +
                ", currentLeader=" + currentLeader +
                '}';
    }
}
