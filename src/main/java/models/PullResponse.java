package models;

import com.google.gson.annotations.Expose;

import java.util.List;

/**
 * Responsible for holding the data being sent as part of the response to the pull request
 *
 * @author Palak Jain
 */
public class PullResponse {
    @Expose
    private int nextOffset;
    @Expose
    private int numOfLogs;
    @Expose
    private List<byte[]> entries;

    public PullResponse(int nextOffset, int numOfLogs, List<byte[]> entries) {
        this.nextOffset = nextOffset;
        this.numOfLogs = numOfLogs;
        this.entries = entries;
    }

    /**
     * Get the next offset to request from for the consumer
     */
    public int getNextOffset() {
        return nextOffset;
    }

    /**
     * Get the numOfLogs being passed as part of the request
     */
    public int getNumOfLogs() {
        return numOfLogs;
    }

    /**
     * Get the entry at the given index
     */
    public byte[] getData(int index) {
        byte[] data = null;

        if (entries.size() > 0) {
            data = entries.get(index);
        }

        return data;
    }
}
