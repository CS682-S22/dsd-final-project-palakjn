package controllers;

import configuration.Constants;
import models.Connections;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import consensus.controllers.CacheManager;
import utils.PacketHandler;

import java.io.IOException;
import java.net.Socket;

/**
 * Responsible for handling common services which a host needs like sending ACK, NACK, etc.
 *
 * @author Palak Jain
 */
public class NodeService {
    private final Logger logger = LogManager.getLogger(NodeService.class);

    /**
     * Open connection with the host
     */
    public Connection connect(String address, int port) {
        Connection connection = null;

        try {
            Socket socket = new Socket(address, port);
            logger.info(String.format("[%s] Successfully connected to the server %s:%d.", CacheManager.getLocal().toString(), address, port));

            connection = Connections.getConnection(socket, address, port);;
            if (!connection.openConnection()) {
                connection = null;
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s] Fail to make connection with the server %s:%d.", CacheManager.getLocal().toString(), address, port), exception.getMessage());
        }

        return connection;
    }

    /**
     * Send REDIRECT packet to the producer/consumer as the current server is not leader
     */
    public void sendLeaderInfo(Host client, int seqNum) {
        Host leader = null;
        int leaderId = CacheManager.getCurrentLeader();

        if (leaderId != -1) {
            leader = CacheManager.getNeighbor(leaderId);
        }

        if (leader != null) {
            logger.info(String.format("[%s] [Follower] Received the request from consumer/producer %s. Sending leader %s[%d] information to the consumer/producer.", CacheManager.getLocal().toString(), client.toString(), leader.toString(), leaderId));
            Packet<Host> response = new Packet<>(Constants.RESPONSE_STATUS.REDIRECT.ordinal(), leader);
            byte[] responseBytes = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.RESP, response, seqNum, client);
            client.send(responseBytes);
        } else {
            logger.warn(String.format("[%s] Leader information not found. Sending NACK to the consumer/producer %s.", CacheManager.getLocal().toString(), client.toString()));
            sendNACK(client, Constants.REQUESTER.SERVER, seqNum);
        }
    }

    /**
     * Send ELECTION packet to the producer/consumer as the election process going on
     */
    public void sendWaitToClient(Host client, int seqNum) {
        logger.warn(String.format("[%s] Server found the crash of the leader. In ELECTION mode. Sending ELECTION to the consumer %s.", CacheManager.getLocal().toString(), client.toString()));
        Packet<?> response = new Packet<>(Constants.RESPONSE_STATUS.ELECTION.ordinal());
        byte[] responseBytes = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.RESP, response, seqNum, client);
        client.send(responseBytes);
    }

    /**
     * Send negative acknowledgment response to the host
     */
    public void sendNACK(Host host, Constants.REQUESTER requester, int seqNum) {
        byte[] acknowledgement = PacketHandler.createNACK(requester, seqNum, host);
        host.send(acknowledgement);
    }

    /**
     * Send acknowledgment response to the host
     */
    public void sendACK(Host host, Constants.REQUESTER requester, int seqNum) {
        byte[] acknowledgement = PacketHandler.createACK(requester, seqNum, host);
        host.send(acknowledgement);
        logger.debug(String.format("[%s] Send ACK to the host %s.", CacheManager.getLocal().toString(), host.toString()));
    }
}
