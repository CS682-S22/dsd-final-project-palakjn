package configuration;

/**
 * Responsible for holding constant values to use.
 *
 * @author Palak Jain
 */
public class Constants {
    public static final int START_VALID_PORT = 1700;
    public static final int END_VALID_PORT = 1724;
    public static int NUM_OF_THREADS = 50;
    public static int PRODUCER_WAIT_TIME = 8000;
    public static int CONSUMER_WAIT_TIME = 500;
    public static int CLIENT_SLEEP_TIME = 500;
    public static int SUFFIX_BATCH_SIZE = 10;
    public static int REPLICATION_PERIOD = 200;
    public static int ELECTION_MIN_TIME = 2000;
    public static int ELECTION_MAX_TIME = 3000;
    public static int FAULT_DETECTOR_MIN_VALUE = 2000;
    public static int FAULT_DETECTOR_MAX_VALUE = 3000;
    public static int HEARTBEAT_TIMEOUT_THRESHOLD = 1500;

    public enum ROLE {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    public enum REQUESTER {
        PRODUCER,
        SERVER,
        CONSUMER
    }

    public enum HEADER_TYPE {
        REQ,
        RESP,
        DATA,
        ACK,
        NACK,
        VOTE_REQ,
        VOTE_RESP,
        ENTRY_REQ,
        ENTRY_RESP,
        PULL_REQ,
        PULL_RESP
    }

    public enum RESPONSE_STATUS {
        OK,
        REDIRECT,
        ELECTION,
        NOT_OK
    }
}
