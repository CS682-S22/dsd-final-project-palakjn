package server.controllers;

import application.Constants;
import controllers.Connection;
import controllers.NodeService;
import models.Host;
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
        byte[] data = PacketHandler.getData(packet);

        if (data != null) {
            if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
                //Appending the log to the local
                if(CacheManager.addEntry(data)) {
                    //Updating acknowledgement of the packets received by current leader
                    CacheManager.setAckedLength(CacheManager.getLocal().getId(), CacheManager.getLogLength());

                    //Sending the same packet as AppendEntries to all the followers
                    //TODO: Replication
                }
            } else if (CacheManager.getCurrentRole() == Constants.ROLE.FOLLOWER.ordinal()) {
                Host leader = null;
                int leaderId = CacheManager.getCurrentLeader();

                if (leaderId != -1) {
                    leader = CacheManager.getMember(leaderId);
                }

                if (leader != null) {
                    logger.info(String.format("[%s] [Follower] Received the log from client %s. Sending leader %s information to the client.", CacheManager.getLocal().toString(), connection.getDestination().toString(), leader.toString()));
                    //TODO: Send Leader info to the client
                } else {
                    logger.warn(String.format("[%s] Leader information not found. Sending NACK to the client %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));
                    nodeService.sendNACK(connection, Constants.REQUESTER.SERVER, seqNum, connection.getDestination());
                }
            } else if (CacheManager.getCurrentRole() == Constants.ROLE.CANDIDATE.ordinal()) {
                logger.warn(String.format("[%s] Server found the crash of the leader. In ELECTION mode. Sending NACK to the client %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));
                nodeService.sendNACK(connection, Constants.REQUESTER.SERVER, seqNum, connection.getDestination());
            }
        }
    }
}