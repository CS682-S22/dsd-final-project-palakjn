package application;

/**
 * Responsible for holding constant values to use.
 *
 * @author Palak Jain
 */
public class Constants {
    public static final int START_VALID_PORT = 1700;
    public static final int END_VALID_PORT = 1724;
    public static int NUM_OF_THREADS = 50;

    public enum ROLE {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    public enum REQUESTER {
        CLIENT,
        SERVER
    }

    public enum HEADER_TYPE {
        REQ,
        RESP,
        DATA,
        ACK,
        NACK
    }

    public enum PACKET_TYPE {
        VOTE,
        APPEND_ENTRIES,
        RESP
    }

    public enum RESPONSE_STATUS {
        REDIRECT,
        ELECTION
    }
}
