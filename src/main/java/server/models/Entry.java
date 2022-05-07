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

    public Entry(int term, int fromOffset, int toOffset) {
        this.term = term;
        this.fromOffset = fromOffset;
        this.toOffset = toOffset;
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

    @Override
    public String toString() {
        return "Entry{" +
                "term=" + term +
                ", fromOffset=" + fromOffset +
                ", toOffset=" + toOffset +
                '}';
    }
}
