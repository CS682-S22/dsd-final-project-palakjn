package consensus.controllers;

import configuration.Constants;
import consensus.models.AppendEntriesRequest;
import consensus.models.AppendEntriesResponse;
import consensus.models.Entry;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
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
    public void appendEntries(AppendEntriesRequest appendEntriesRequest) {
        int currentTerm = CacheManager.getTerm();
        Host leader = CacheManager.getNeighbor(appendEntriesRequest.getLeaderId());

        if (appendEntriesRequest.getTerm() > currentTerm) {
            logger.info(String.format("[%s] Received AppendEntries from new leader %s with the new term %d. Old term: %d", CacheManager.getLocal().toString(), leader.toString(), appendEntriesRequest.getTerm(), currentTerm));
            CacheManager.setTerm(appendEntriesRequest.getTerm());
            CacheManager.setVoteFor(-1);
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

        if (appendEntriesRequest.getTerm() == currentTerm && logOk) {
            logger.info(String.format("[%s] Accepted %d number of logs from leader %s. Writing to local logs if not duplicate", CacheManager.getLocal().toString(), appendEntriesRequest.getLength(), leader.toString()));
            writeLogs(appendEntriesRequest);
            ack = appendEntriesRequest.getPrefixLen() + appendEntriesRequest.getLength();
            isSuccess = true;
        } else {
            logger.warn(String.format("[%s] Rejecting the logs from the leader %s. CurrentTerm: %d, LeaderTerm: %d, log.length: %d," +
                    "prefixLen: %d, logs[prefixLen].term: %d, prefixTerm: %d", CacheManager.getLocal().toString(), leader.toString(), currentTerm, appendEntriesRequest.getTerm(),
                    CacheManager.getLogLength(), appendEntriesRequest.getPrefixLen(), CacheManager.getEntry(appendEntriesRequest.getPrefixLen() - 1).getTerm(), appendEntriesRequest.getPrefixTerm()));
        }

        AppendEntriesResponse response = new AppendEntriesResponse(currentTerm, ack, CacheManager.getLocal().getId(), isSuccess);
        Packet<AppendEntriesResponse> packet = new Packet<>(Constants.RESPONSE_STATUS.OK.ordinal(), response);
        byte[] responseBytes = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.ENTRY_RESP, packet, 0, leader);
        leader.send(responseBytes);
    }

    /**
     * Receiving the acknowledgement from the follower
     */
    public void processAcknowledgement(AppendEntriesResponse appendEntriesResponse) {
        int currentTerm = CacheManager.getTerm();

        Host follower = CacheManager.getNeighbor(appendEntriesResponse.getNodeId());
        if (appendEntriesResponse.getTerm() == currentTerm && CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
            if (appendEntriesResponse.isSuccess() && appendEntriesResponse.getAck() >= CacheManager.getAckedLength(follower.getId())) {
                logger.info(String.format("[%s] Received an acknowledgement from the follower %s that it has received the total of %d logs till now.", CacheManager.getLocal().toString(), follower.getId(), appendEntriesResponse.getAck()));
                CacheManager.setSentLength(follower.getId(), appendEntriesResponse.getAck());
                CacheManager.setAckedLength(follower.getId(), appendEntriesResponse.getAck());
                //TODO: commitLogEntries()
            } else if (CacheManager.getSentLength(appendEntriesResponse.getNodeId()) > 0) {
                logger.info(String.format("[%s] Need to repair the logs with the follower %s as follower is behind the prefix logs. " +
                        "Previously sent logs from %d position. Now, sending from %d position.", CacheManager.getLocal().toString(), follower.toString(), CacheManager.getSentLength(currentTerm), CacheManager.getSentLength(currentTerm) - 1));
                CacheManager.decrementSentLength(follower.getId());
            }
        } else if (appendEntriesResponse.getTerm() > currentTerm) {
            logger.info(String.format("[%s] Leader's term %d is less that follower %s's term %d. Seems that there is a new leader in the system. " +
                    "Changing role to follower.", CacheManager.getLocal().toString(), currentTerm, follower.toString(), appendEntriesResponse.getTerm()));
            CacheManager.setTerm(appendEntriesResponse.getTerm());
            CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
            CacheManager.setVoteFor(-1);
            Election.stopTimer();
            FaultDetector.startTimer();
        }
    }

    /**
     * Write the logs received from leader to local
     */
    private void writeLogs(AppendEntriesRequest appendEntriesRequest) {
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
                    logger.info(String.format("[%s] Repairing logs. Removing logs from %d index to %d index as those entries not found in leader %s log.", CacheManager.getLocal().toString(), index2, CacheManager.getLogLength(), leader.toString()));
                    CacheManager.removeEntryFrom(index2);
                    break;
                }
            }
        }

        if ((appendEntriesRequest.getPrefixLen() + appendEntriesRequest.getLength()) > CacheManager.getLogLength()) {
            while (index < appendEntriesRequest.getLength()) {
                Entry entry = appendEntriesRequest.getSuffix(index);

                if(CacheManager.addEntry(entry.getData(), entry.getClientId(), entry.getReceivedOffset())) {
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
            for (index = commitLength - 1; index < appendEntriesRequest.getCommitLength(); index++) {
                //TODO: Deliver log[i] message to the application
            }

            //TODO: Set only those which are successfully applied to state machine
            CacheManager.setCommitLength(appendEntriesRequest.getCommitLength());;
        }
    }

    /**
     * Replicate the logs which are not sent before to the follower
     */
    private static void replicate(int currentTerm, Host follower) {

        //Getting the length of the logs already being sent to the follower before
        int prefixLen = CacheManager.getSentLength(follower.getId());

        //Getting batch of 10 logs metadata from the prefixLen
        List<Entry> entries = CacheManager.getEntries(prefixLen);

        int prefixTerm = 0;

        if (prefixLen > 0) {
            prefixTerm = CacheManager.getEntry(prefixLen - 1).getTerm();
        }

        List<Entry> suffix = new ArrayList<>();

        for (Entry entry : entries) {
            byte[] data = FileManager.read(entry.getFromOffset(), (entry.getToOffset() + 1) - entry.getFromOffset());

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
