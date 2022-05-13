package consensus.models;

import com.google.gson.annotations.Expose;

/**
 * Responsible for holding the information about the log stored at the log index.
 * For example: term - when the log was being written
 *              fromOffset - starting offset to read from
 *              toOffset - offset of the log to read till
 *
 * @author Palak Jain
 */
public class Entry {
    @Expose
    private int term;
    @Expose
    private int fromOffset;
    @Expose
    private int toOffset;
    @Expose
    private String clientId;
    @Expose
    private int receivedOffset;
    @Expose
    private boolean isCommitted;
    @Expose
    private byte[] data;

    public Entry(int term, int fromOffset, int toOffset, String clientId, int receivedOffset, boolean isCommitted) {
        this.term = term;
        this.fromOffset = fromOffset;
        this.toOffset = toOffset;
        this.clientId = clientId;
        this.receivedOffset = receivedOffset;
        this.isCommitted = isCommitted;
    }

    public Entry(Entry entry) {
        this.term = entry.getTerm();
        this.fromOffset = entry.getFromOffset();
        this.toOffset = entry.getToOffset();
        this.clientId = entry.getClientId();
        this.receivedOffset = entry.getReceivedOffset();
        this.data = entry.getData();
        this.isCommitted = entry.isCommitted();
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

    /**
     * Get whether the entry is being committed
     */
    public boolean isCommitted() {
        return isCommitted;
    }

    /**
     * Set isCommitted to true
     */
    public void setCommitted() {
        isCommitted = true;
    }

    /**
     * Get the content of the log
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Set the content of the log
     */
    public void setData(byte[] data) {
        this.data = data;
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
