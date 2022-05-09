package server.models;

/**
 * Holds the information like how many data received from client by the current server.
 *
 * @author Palak Jain
 */
public class SyncState {
    private String clientId;
    private int receivedCount;

    public SyncState(String clientId, int receivedCount) {
        this.clientId = clientId;
        this.receivedCount = receivedCount;
    }

    public SyncState() {}

    /**
     * Get the id of the client
     */
    public String getClientId() {
        return clientId;
    }
    /**
     * Set the id of the client
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
    /**
     * Get the number of data received from the client so far
     */
    public int getReceivedCount() {
        return receivedCount;
    }
    /**
     * Set the number of data received from the client so far
     */
    public void setReceivedCount(int receivedCount) {
        this.receivedCount = receivedCount;
    }
}
