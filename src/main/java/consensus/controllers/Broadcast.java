package consensus.controllers;

import configuration.Constants;
import controllers.Connection;
import controllers.NodeService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.PacketHandler;

/**
 * Responsible for processing the log received from the client.
 *
 * @author Palak Jain
 */
public class Broadcast {
    private static final Logger logger = LogManager.getLogger(Broadcast.class);
    private NodeService nodeService;

    public Broadcast() {
        nodeService = new NodeService();
    }

    /**
     * Process the log received from the client. Append the log to the disk if leader else forward the log to the leader
     * Send NACK if
     */
    public void process(Connection connection, byte[] packet, int seqNum) {
        if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
            int lastReceivedOffset = CacheManager.getLastReceivedOffset(connection.getDestination().toString());
            if (seqNum > lastReceivedOffset) {
                byte[] data = PacketHandler.getData(packet);

                if (data != null) {
                    //Appending the log to the local
                    if (CacheManager.addEntry(data, CacheManager.getTerm(), connection.getDestination().toString(), seqNum)) {
                        //Updating acknowledgement of the packets received by current leader
                        CacheManager.setAckedLength(CacheManager.getLocal().getId(), CacheManager.getLogLength());
                        logger.info(String.format("[%s] Server added client %s log with seqNum %d.", CacheManager.getLocal().toString(), connection.getDestination().toString(), seqNum));
                    } else {
                        logger.warn(String.format("[%s] Not able to write log with the offset %d from the client %s.", CacheManager.getLocal().toString(), seqNum, connection.getDestination().toString()));
                        nodeService.sendNACK(connection.getDestination(), Constants.REQUESTER.SERVER, seqNum);
                    }
                }
            } else {
                logger.warn(String.format("[%s] Server received duplicate DATA request from client %s to hold log at the starting offset %d. Server already holds the client logs till offset %d.", CacheManager.getLocal().toString(), connection.getDestination().toString(), seqNum, lastReceivedOffset));
                nodeService.sendACK(connection.getDestination(), Constants.REQUESTER.SERVER, seqNum);
            }
        } else if (CacheManager.getCurrentRole() == Constants.ROLE.FOLLOWER.ordinal()) {
            nodeService.sendLeaderInfo(connection.getDestination(), seqNum);
        } else if (CacheManager.getCurrentRole() == Constants.ROLE.CANDIDATE.ordinal()) {
            nodeService.sendWaitToClient(connection.getDestination(), seqNum);
        }
    }
}
