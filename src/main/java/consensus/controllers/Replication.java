package consensus.controllers;

import configuration.Constants;
import consensus.controllers.database.EntryDB;
import consensus.models.AppendEntriesRequest;
import consensus.models.AppendEntriesResponse;
import consensus.models.Entry;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import stateMachine.Broker;
import utils.FileManager;
import utils.PacketHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Periodically send the new logs if any to all the followers if the server is the leader
 *
 * @author Palak Jain
 */
public class Replication {
    private static final Logger logger = LogManager.getLogger(Replication.class);
    private static Timer timer;

    private static final Object appendLock = new Object();
    private static final Object commitLock = new Object();

    private Replication() {}

    /**
     * Start the timer to replicate the logs to followers if the current server is the leader
     */
    public static void startTimer() {
        timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                ThreadContext.put("module", CacheManager.getLocal().getName());
                if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
                    int currentTerm = CacheManager.getTerm();
                    List<Host> followers = CacheManager.getNeighbors();
                    logger.info(String.format("[%s] Sending AppendEntry packet to %d followers.", CacheManager.getLocal().toString(), followers.size()));

                    for (Host follower : followers) {
                        replicate(currentTerm, follower);
                    }

                    restart();
                } else {
                    logger.info(String.format("[%s] Server is not anymore leader. Stopping replicating logs to followers. Current role: %s", CacheManager.getLocal().toString(), Constants.ROLE.values()[CacheManager.getCurrentRole()].name()));
                    stopTimer();
                }
            }
        };

        timer.schedule(task, Constants.REPLICATION_PERIOD);
    }

    /**
     * Restart the timer
     */
    public static void restart() {
        stopTimer();
        startTimer();
    }

    /**
     * Stop the timer
     */
    public static void stopTimer() {
        timer.cancel();
    }

    /**
     * Accept the new logs from the leader if last received log term and index matches with the leader
     */
    public static void appendEntries(AppendEntriesRequest appendEntriesRequest) {
        synchronized (appendLock) {
            int currentTerm = CacheManager.getTerm();
            Host leader = CacheManager.getNeighbor(appendEntriesRequest.getLeaderId());

            if (appendEntriesRequest.getTerm() > currentTerm) {
                logger.info(String.format("[%s] Received AppendEntries from new leader %s with the new term %d. Old term: %d", CacheManager.getLocal().toString(), leader.toString(), appendEntriesRequest.getTerm(), currentTerm));
                currentTerm = CacheManager.setTerm(appendEntriesRequest.getTerm());
                CacheManager.setVoteFor(-1, false);
                Election.stopTimer();
                FaultDetector.startTimer();
            }

            if (appendEntriesRequest.getTerm() == currentTerm) {
                CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
                CacheManager.setCurrentLeader(appendEntriesRequest.getLeaderId());
                FaultDetector.heartBeatReceived();
            }

            boolean logOk = CacheManager.getLogLength() >= appendEntriesRequest.getPrefixLen() &&
                    (appendEntriesRequest.getPrefixLen() == 0 || CacheManager.getEntry(appendEntriesRequest.getPrefixLen() - 1).getTerm() == appendEntriesRequest.getPrefixTerm());

            int ack = 0;
            boolean isSuccess = false;

            int termAtPrefixLen = -1;

            if (appendEntriesRequest.getPrefixLen() > 0) {
                Entry entry = CacheManager.getEntry(appendEntriesRequest.getPrefixLen() - 1);
                if (entry != null) termAtPrefixLen = entry.getTerm();
            }

            if (appendEntriesRequest.getTerm() == currentTerm && logOk) {
                logger.info(String.format("[%s] Accepted %d number of logs from leader %s.  CurrentTerm: %d, LeaderTerm: %d, log.length: %d,\" +\n" +
                        "                                \"prefixLen: %d, logs[prefixLen].term: %d, prefixTerm: %d", CacheManager.getLocal().toString(), appendEntriesRequest.getLength(), leader.toString(), currentTerm, appendEntriesRequest.getTerm(),
                        CacheManager.getLogLength(), appendEntriesRequest.getPrefixLen(), termAtPrefixLen, appendEntriesRequest.getPrefixTerm()));
                writeLogs(appendEntriesRequest);
                ack = appendEntriesRequest.getPrefixLen() + appendEntriesRequest.getLength();
                isSuccess = true;
            } else {
                logger.warn(String.format("[%s] Rejecting the logs from the leader %s. CurrentTerm: %d, LeaderTerm: %d, log.length: %d," +
                                "prefixLen: %d, logs[prefixLen].term: %d, prefixTerm: %d", CacheManager.getLocal().toString(), leader.toString(), currentTerm, appendEntriesRequest.getTerm(),
                        CacheManager.getLogLength(), appendEntriesRequest.getPrefixLen(), termAtPrefixLen, appendEntriesRequest.getPrefixTerm()));
            }

            AppendEntriesResponse response = new AppendEntriesResponse(currentTerm, ack, CacheManager.getLocal().getId(), isSuccess);
            Packet<AppendEntriesResponse> packet = new Packet<>(Constants.RESPONSE_STATUS.OK.ordinal(), response);
            byte[] responseBytes = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.ENTRY_RESP, packet, 0, leader);
            leader.send(responseBytes);
        }
    }

    /**
     * Receiving the acknowledgement from the follower
     */
    public static void processAcknowledgement(AppendEntriesResponse appendEntriesResponse) {
        int currentTerm = CacheManager.getTerm();

        Host follower = CacheManager.getNeighbor(appendEntriesResponse.getNodeId());
        if (appendEntriesResponse.getTerm() == currentTerm && CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
            if (appendEntriesResponse.isSuccess() && appendEntriesResponse.getAck() >= CacheManager.getAckedLength(follower.getId())) {
                logger.info(String.format("[%s] Received an acknowledgement from the follower %s that it has received the total of %d logs till now.", CacheManager.getLocal().toString(), follower.toString(), appendEntriesResponse.getAck()));
                CacheManager.setSentLength(follower.getId(), appendEntriesResponse.getAck());
                CacheManager.setAckedLength(follower.getId(), appendEntriesResponse.getAck());
                commitLogEntries();
            } else if (CacheManager.getSentLength(follower.getId()) > 0) {
                logger.info(String.format("[%s] Need to repair the logs with the follower %s as follower is behind the prefix logs. " +
                        "Previously sent logs from %d position. Now, sending from %d position.", CacheManager.getLocal().toString(), follower.toString(), CacheManager.getSentLength(follower.getId()), CacheManager.getSentLength(follower.getId()) - 1));
                CacheManager.decrementSentLength(follower.getId());
            } else {
                logger.info(String.format("[%s] Received an %d acknowledgement from the follower %s but previously received acknowledgement was %d", CacheManager.getLocal().toString(), appendEntriesResponse.getAck(), follower.toString(), CacheManager.getAckedLength(follower.getId())));
            }
        } else if (appendEntriesResponse.getTerm() > currentTerm) {
            logger.info(String.format("[%s] Leader's term %d is less that follower %s's term %d. Seems that there is a new leader in the system. " +
                    "Changing role to follower.", CacheManager.getLocal().toString(), currentTerm, follower.toString(), appendEntriesResponse.getTerm()));
            CacheManager.setTerm(appendEntriesResponse.getTerm());
            CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
            CacheManager.setVoteFor(-1, false);
            Election.stopTimer();
            FaultDetector.startTimer();
        }

    }

    /**
     * Write the logs received from leader to local
     */
    private static void writeLogs(AppendEntriesRequest appendEntriesRequest) {
        Host leader = CacheManager.getNeighbor(appendEntriesRequest.getLeaderId());
        int index = 0;

        if (appendEntriesRequest.getLength() > 0 && CacheManager.getLogLength() > appendEntriesRequest.getPrefixLen()) {
            int index2 = appendEntriesRequest.getPrefixLen();

            while (index < appendEntriesRequest.getLength() && index2 < CacheManager.getLogLength()) {
                if (appendEntriesRequest.getSuffix(index).getTerm() == CacheManager.getEntry(index2).getTerm()) {
                    index++;
                    index2++;
                    logger.debug(String.format("[%s] Found same entry in current server log at the index equal to the leader %s's log index.", CacheManager.getLocal().toString(), leader.toString()));
                } else {
                    logger.info(String.format("[%s] Repairing logs. Removing logs from %d index to %d index as those entries not found in leader %s log.", CacheManager.getLocal().toString(), index2, CacheManager.getLogLength() - 1, leader.toString()));
                    CacheManager.removeEntryFrom(index2);
                    break;
                }
            }
        }

        if ((appendEntriesRequest.getPrefixLen() + appendEntriesRequest.getLength()) > CacheManager.getLogLength()) {
            while (index < appendEntriesRequest.getLength()) {
                Entry entry = appendEntriesRequest.getSuffix(index);

                if(CacheManager.addEntry(entry.getData(), entry.getTerm(), entry.getClientId(), entry.getReceivedOffset())) {
                    logger.info(String.format("[%s] Saved %d bytes of data from leader %s.", CacheManager.getLocal().toString(), entry.getData().length, leader.toString()));
                } else {
                    logger.warn(String.format("[%s] Fail to write %d bytes of data from leader %s.", CacheManager.getLocal().toString(), entry.getData().length, leader.toString()));
                    break;
                }

                index++;
            }
        }

        int commitLength = CacheManager.getCommitLength();
        if (appendEntriesRequest.getCommitLength() > commitLength) {
            logger.info(String.format("[%s] Leader has committed few entries. CommitLength: %d.  Leader's commit length: %d. Sending logs to Broker",
                    CacheManager.getLocal().toString(), commitLength, appendEntriesRequest.getCommitLength()));
            while (commitLength < appendEntriesRequest.getCommitLength()) {
                if (sendToBroker(commitLength)) {
                    commitLength++;
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Commit log entries after receiving acknowledgements from the majority of the nodes
     */
    private static void commitLogEntries() {
        synchronized (commitLock) {
            int logLength = CacheManager.getLogLength();
            int commitLength = CacheManager.getCommitLength();
            List<Host> nodes = CacheManager.getMembers();

            logger.info(String.format("[%s] [Leader] %s logs as commitLength: %d and logLength: %d.", CacheManager.getLocal().toString(),
                    commitLength < logLength ? "Will try to commit" : "Will not commit", commitLength, logLength));

            while (commitLength < logLength) {
                int ack = 0;

                for (Host node : nodes) {
                    if (CacheManager.getAckedLength(node.getId()) > commitLength) {
                        logger.info(String.format("[%s] Follower %s acknowledged for %d packets where commit length is %d.", CacheManager.getLocal().toString(), node.toString(), CacheManager.getAckedLength(node.getId()), commitLength));
                        ack = ack + 1;
                    }
                }

                if (ack >= Math.ceil((CacheManager.getMemberCount() + 1) / 2.0)) {
                    if (sendToBroker(commitLength)) {
                        commitLength++;
                    } else {
                        break;
                    }
                } else {
                    logger.info(String.format("[%s] Not committing as received only %d acknowledgments but wanted least %f acknowledgements", CacheManager.getLocal().toString(), ack, Math.ceil((CacheManager.getMemberCount() + 1) / 2.0)));
                    break;
                }
            }
        }
    }

    /**
     * Sending log to the broker after being replicated to the majority of nodes
     */
    private static boolean sendToBroker(int commitLength) {
        logger.info(String.format("[%s] Committing log number %d.", CacheManager.getLocal().toString(), commitLength));
        boolean isSuccess = false;

        Entry entry = CacheManager.getEntry(commitLength);
        if (entry != null) {
            byte[] data = FileManager.read(CacheManager.getLocation(), entry.getFromOffset(), (entry.getToOffset() + 1) - entry.getFromOffset());
            if (data != null && Broker.publish(entry.getClientId(), data, entry.getReceivedOffset())) {
                commitLength++;
                CacheManager.setCommitLength(commitLength);
                entry.setCommitted();
                EntryDB.setCommitted(entry.getFromOffset(), true);
                isSuccess = true;
            }
        }
        return isSuccess;
    }

    /**
     * Replicate the logs which are not sent before to the follower
     */
    private static void replicate(int currentTerm, Host follower) {
        //Getting the length of the logs already being sent to the follower before
        int prefixLen = CacheManager.getSentLength(follower.getId());
        logger.info(String.format("[%s] Need to send logs from the prefixLen: %d to the follower: %s", CacheManager.getLocal().toString(), prefixLen, follower.toString()));

        //Getting batch of 10 logs metadata from the prefixLen
        List<Entry> entries = CacheManager.getEntries(prefixLen, Constants.SUFFIX_BATCH_SIZE, false);

        int prefixTerm = 0;

        if (prefixLen > 0) {
            prefixTerm = CacheManager.getEntry(prefixLen - 1).getTerm();
        }

        List<Entry> suffix = new ArrayList<>();

        for (Entry entry : entries) {
            byte[] data = FileManager.read(CacheManager.getLocation(), entry.getFromOffset(), (entry.getToOffset() + 1) - entry.getFromOffset());

            if (data != null) {
                Entry copyEntry = new Entry(entry);
                copyEntry.setData(data);
                suffix.add(copyEntry);
            } else {
                break;
            }
        }

        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(CacheManager.getLocal().getId(), currentTerm, prefixLen, prefixTerm, CacheManager.getCommitLength(), suffix);
        Packet<AppendEntriesRequest> packet = new Packet<>(Constants.RESPONSE_STATUS.OK.ordinal(), appendEntriesRequest);

        follower.send(PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.ENTRY_REQ, packet, 0, follower));
        logger.info(String.format("[%s] Send AppendEntries packet to the follower %s with %d new logs [PrefixLen: %d. PrefixTerm: %d].", CacheManager.getLocal().toString(), follower.toString(), suffix.size(), prefixLen, prefixTerm));
    }
}
