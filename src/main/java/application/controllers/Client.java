package application.controllers;

import application.configuration.Config;
import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import controllers.Connection;
import models.Header;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import utils.JSONDeserializer;
import utils.PacketHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class Client {
    protected BlockingQueue<byte[]> responseQueue;
    protected Logger logger;
    protected Config config;

    public Client(Logger logger, Config config) {
        responseQueue = new LinkedBlockingDeque<>();
        this.logger = logger;
        this.config = config;
    }

    /**
     * Listen for the response either from consumer service/leader of the consensus system
     */
    public void listenForResponse(Connection connection) {
        ThreadContext.put("module", config.getLocal().getName());

        while (connection.isOpen()) {
            byte[] data = connection.receive();
            if (data != null) {
                logger.info(String.format("[%s] Received the response from the leader: %s", config.getLocal().toString(), connection.getDestination().toString()));

                try {
                    responseQueue.put(data);
                } catch (InterruptedException e) {
                    logger.error(String.format("[%s] Unable to add the response received from the leader to the queue.", config.getLocal().toString()), e);
                }
            } else if (!connection.isOpen()) {
                logger.warn(String.format("[%s] Connection is closed by the leader.", config.getLocal().toString()));
            }
        }
    }

    /**
     * Process the response received from the leader
     */
    protected boolean processResponse(byte[] response, int offset) {
        logger.info(String.format("[%s] Received the response from the queue.", config.getLocal().toString()));
        boolean isSuccess = false;

        Header.Content header = PacketHandler.getHeader(response);

        if (header != null) {
            if (header.getSeqNum() == offset) {
                if (header.getType() == Constants.HEADER_TYPE.ACK.ordinal() || header.getType() == Constants.HEADER_TYPE.PULL_RESP.ordinal()) {
                    logger.info(String.format("[%s] Received an acknowledgement/PULL Response for the packet with sequence number %d.", config.getLocal().toString(), offset));
                    isSuccess = true;
                } else if (header.getType() == Constants.HEADER_TYPE.NACK.ordinal()) {
                    logger.warn(String.format("[%s] Received negative acknowledgement for the packet with sequence number %d. Retrying", config.getLocal().toString(), offset));
                } else if (header.getType() == Constants.HEADER_TYPE.RESP.ordinal()) {
                    byte[] body = PacketHandler.getData(response);

                    if (body != null) {
                        Packet<Host> packet = JSONDeserializer.deserializePacket(body, new TypeToken<Packet<Host>>() {}.getType());

                        if (packet != null) {
                            if (packet.getStatus() == Constants.RESPONSE_STATUS.REDIRECT.ordinal()) {
                                if (packet.getObject() != null) {
                                    logger.info(String.format("[%s] Leader changed. Sending data to the server %s with the sequence number %d.", config.getLocal().toString(), packet.getObject().toString(), offset));
                                    config.setLeader(packet.getObject());
                                } else {
                                    logger.warn(String.format("[%s] Leader changed but not found the leader info in the packet. Re-sending the same packet to the server %s with the seqNum as %d.", config.getLocal().toString(), config.getLeader().toString(), offset));
                                }
                            } else if (packet.getStatus() == Constants.RESPONSE_STATUS.ELECTION.ordinal()) {
                                logger.info(String.format("[%s] Leader changed. Election process going on. Sleeping for %d time before retrying for the seqNum %d.", config.getLocal().toString(), Constants.CLIENT_SLEEP_TIME, offset));
                                try {
                                    Thread.sleep(Constants.CLIENT_SLEEP_TIME);
                                } catch (InterruptedException e) {
                                    logger.error(String.format("[%s] Error while sleeping for %d amount of time.", config.getLocal().toString(), Constants.CLIENT_SLEEP_TIME), e);
                                }
                            } else {
                                logger.warn(String.format("[%s] Received invalid response status %d from the server %s.", config.getLocal().toString(), packet.getStatus(), config.getLeader().toString()));
                            }
                        }
                    }
                }
            } else {
                logger.info(String.format("[%s] Received the response with the sequence number: %d. Expected sequence number: %d from leader %s.", config.getLocal().toString(), header.getSeqNum(), offset, config.getLeader().toString()));
            }
        }

        return isSuccess;
    }

    /**
     * Read the log line by line and send it to the leader of the consensus system
     */
    public abstract void send();

    /**
     * Pulling n number of logs from the arbitrary offset from the leader
     */
    public abstract void pull();
}
