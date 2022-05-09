package server.models;

/**
 * Responsible for holding the information about the log stored at the log index.
 * For example: term - when the log was being written
 *              fromOffset - starting offset to read from
 *              toOffset - offset of the log to read till
 *
 * @author Palak Jain
 */
public class Entry {
    private int term;
    private int fromOffset;
    private int toOffset;
    private String clientId;
    private int receivedOffset;

    public Entry(int term, int fromOffset, int toOffset, String clientId, int receivedOffset) {
        this.term = term;
        this.fromOffset = fromOffset;
        this.toOffset = toOffset;
        this.clientId = clientId;
        this.receivedOffset = receivedOffset;
    }

    /**
     * Get the term
     */
    public int getTerm() {
        return term;
    }

    /**
     * Set the term
     */
    public void setTerm(int term) {
        this.term = term;
    }

    /**
     * Get the starting offset
     */
    public int getFromOffset() {
        return fromOffset;
    }

    /**
     * Set the starting offset
     */
    public void setFromOffset(int offset) {
        this.fromOffset = offset;
    }

    /**
     * Get the ending offset
     */
    public int getToOffset() {
        return toOffset;
    }

    /**
     * Set the ending offset
     */
    public void setToOffset(int toOffset) {
        this.toOffset = toOffset;
    }

    /**
     * Get the client id who has sent the current entry
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Set the client id who has sent the current entry
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Get the actual starting offset which is passed by the client
     */
    public int getReceivedOffset() {
        return receivedOffset;
    }

    /**
     * Set the actual starting offset which is passed by the client
     */
    public void setReceivedOffset(int receivedOffset) {
        this.receivedOffset = receivedOffset;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "term=" + term +
                ", fromOffset=" + fromOffset +
                ", toOffset=" + toOffset +
                ", clientId=" + clientId +
                ", receivedOffset=" + receivedOffset +
                '}';
    }
}
