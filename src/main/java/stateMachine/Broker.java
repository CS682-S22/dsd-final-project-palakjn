package stateMachine;

import configuration.Constants;
import consensus.controllers.CacheManager;
import consensus.models.Entry;
import controllers.NodeService;
import models.Host;
import models.Packet;
import models.PullRequest;
import models.PullResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.FileManager;
import utils.PacketHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for getting distinct logs from producer in order after ensuring that it is being replicated to the majority of the servers.
 * Send logs to consumer on the request of pull request
 *
 * @author Palak Jain
 */
public class Broker {
    private static final Logger logger = LogManager.getLogger(Broker.class);
    private static NodeService nodeService = new NodeService();

    /**
     * Save log to the local from the consumer
     */
    public static boolean publish(String clientId, byte[] data, int seqNum) {
        boolean isCommitted = false;
        Host client = new Host(clientId);

        int offset = CacheManager.getLastOffset(true);

        boolean isSuccess = FileManager.write(CacheManager.getCommitLocation(), data, offset);

        if (isSuccess) {
            if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
                logger.info(String.format("[%s] [Leader] Committed log at the offset %d and Sending ACK to the producer %s.", CacheManager.getLocal().toString(), offset, clientId));
                nodeService.sendACK(client, Constants.REQUESTER.SERVER, seqNum);
            } else {
                logger.info(String.format("[%s] [Follower] Committed and write log at the offset %d", CacheManager.getLocal().toString(), offset));
            }
            isCommitted = true;
        } else {
            if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
                logger.info(String.format("[%s] [Leader] Unable to commit log at the offset %d and Sending NACK to the producer %s.", CacheManager.getLocal().toString(), offset, clientId));
                nodeService.sendNACK(client, Constants.REQUESTER.SERVER, seqNum);
            } else {
                logger.info(String.format("[%s] [Follower] Unable to commit and write log at the offset %d", CacheManager.getLocal().toString(), offset));
            }
        }

        return isCommitted;
    }

    /**
     * Process the pull request received from the consumer.
     * Send requested number of logs from the request offset if exist
     */
    public static void pull(Host client, PullRequest pullRequest, int seqNum) {
        logger.info(String.format("[%s] Received PULL request from consumer %s to get %d number of logs from offset %d with the seqNum as %d.", CacheManager.getLocal().toString(), client.toString(),
                pullRequest.getNumOfLogs(), pullRequest.getFromOffset(), seqNum));

        if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
            send(client, pullRequest, seqNum);
        } else if (CacheManager.getCurrentRole() == Constants.ROLE.FOLLOWER.ordinal()) {
            nodeService.sendLeaderInfo(client, seqNum);
        } else if (CacheManager.getCurrentRole() == Constants.ROLE.CANDIDATE.ordinal()) {
            nodeService.sendWaitToClient(client, seqNum);
        }
    }

    /**
     * Send the requested number of logs from the given offset to the consumer
     */
    private static void send(Host client, PullRequest pullRequest, int seqNum) {
        int index = CacheManager.getEntryIndex(pullRequest.getFromOffset(), true);

        if (index != -1) {
            List<Entry> entries = CacheManager.getEntries(index, pullRequest.getNumOfLogs(), true);
            List<byte[]> dataToSend = new ArrayList<>();

            int nextOffset = -1;
            logger.info(String.format("[%s] Sending %d number of logs to consumer %s.", CacheManager.getLocal().toString(), entries.size(), client.toString()));
            for (Entry entry : entries) {
                byte[] data = FileManager.read(CacheManager.getCommitLocation(), entry.getFromOffset(), (entry.getToOffset() + 1) - entry.getFromOffset());

                if (data != null) {
                    dataToSend.add(data);
                    nextOffset = entry.getToOffset();
                } else {
                    break;
                }
            }

            PullResponse pullResponse = new PullResponse(nextOffset + 1, dataToSend.size(), dataToSend);
            Packet<PullResponse> packet = new Packet<>(Constants.RESPONSE_STATUS.OK.ordinal(), pullResponse);
            byte[] data = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.PULL_RESP, packet, seqNum, client);
            client.send(data);
            logger.info(String.format("[%s] Send %d number of logs to the consumer %s with next offset as %d.", CacheManager.getLocal().toString(), dataToSend.size(), client.toString(), nextOffset + 1));
        } else {
            PullResponse pullResponse = new PullResponse(pullRequest.getFromOffset(), 0, null);
            Packet<PullResponse> packet = new Packet<>(Constants.RESPONSE_STATUS.NOT_FOUND.ordinal(), pullResponse);
            byte[] data = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.PULL_RESP, packet, seqNum, client);
            client.send(data);
            logger.warn(String.format("[%s] Broker not holding offset %d. Sending NOT FOUND response to consumer %s.", CacheManager.getLocal().toString(), pullRequest.getFromOffset(), client.toString()));
        }
    }
}
