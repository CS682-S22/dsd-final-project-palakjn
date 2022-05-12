package application.controllers;

import application.configuration.Config;
import configuration.Constants;
import controllers.Connection;
import models.Header;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import utils.JSONDeserializer;
import utils.PacketHandler;
import utils.Strings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for reading logs from the file and sending to the leader of the system.
 *
 * @author Palak Jain
 */
public class Producer {
    private BlockingQueue<byte[]> responseQueue;
    private static final Logger logger = LogManager.getLogger(Producer.class);
    private Config config;
    private final Object lock;

    public Producer(Config config) {
        responseQueue = new LinkedBlockingDeque<>();
        lock = new Object();
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
                logger.info(String.format("[%s] Received the response from the consumer: %s", config.getLocal().toString(), connection.getDestination().toString()));

                try {
                    responseQueue.put(data);
                } catch (InterruptedException e) {
                    logger.error(String.format("[%s] Unable to add the response received from the consumer to the queue.", config.getLocal().toString()), e);
                }
            } else if (!connection.isOpen()) {
                logger.warn(String.format("[%s] Connection is closed by the consumer/leader.", config.getLocal().toString()));
            }
        }
    }

    /**
     * Read the log line by line and send it to the leader of the consensus system
     */
    public void send() {
        try (BufferedReader reader = new BufferedReader(new FileReader(config.getLocation()))) {
            String line = reader.readLine();
            int offset = 0;

            while (!Strings.isNullOrEmpty(line)) {
                byte[] data = PacketHandler.createPacket(Constants.REQUESTER.CLIENT, Constants.HEADER_TYPE.DATA, line.getBytes(StandardCharsets.UTF_8), offset, config.getLeader());
                logger.info(String.format("[%s] Sending %d bytes of data to the leader of the system %s.", config.getLocal().toString(), data.length, config.getLeader().toString()));
                config.getLeader().send(data);

                byte[] response = null;

                try {
                    response = responseQueue.poll(Constants.PRODUCER_WAIT_TIME, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error(String.format("[%s] Unable to pull the response received from the consumer from the queue.", config.getLocal().toString()), e);
                }

                if (response != null) {
                    logger.info(String.format("[%s] Received the response from the queue.", config.getLocal().toString()));

                    Header.Content header = PacketHandler.getHeader(response);

                    if (header != null) {
                        if (header.getType() == Constants.HEADER_TYPE.ACK.ordinal()) {
                            logger.info(String.format("[%s] Received an acknowledgement for the packet with sequence number %d.", config.getLocal().toString(), offset));
                            offset += line.length();
                            line = reader.readLine();
                        } else if (header.getType() == Constants.HEADER_TYPE.NACK.ordinal()) {
                            logger.warn(String.format("[%s] Received negative acknowledgement for the packet with sequence number %d. Retrying", config.getLocal().toString(), offset));
                        } else if (header.getType() == Constants.HEADER_TYPE.RESP.ordinal()) {
                            byte[] body = PacketHandler.getData(response);

                            if (body != null) {
                                Packet<?> packet = JSONDeserializer.fromJson(body, Packet.class);

                                if (packet != null) {
                                    if (packet.getStatus() == Constants.RESPONSE_STATUS.REDIRECT.ordinal()) {
                                        if (packet.getObject() != null && packet.getObject() instanceof Host leader) {
                                            logger.info(String.format("[%s] Leader changed. Sending data to the server %s with the sequence number %d.", config.getLocal().toString(), leader.toString(), offset));
                                            config.setLeader(leader);
                                        } else {
                                            logger.warn(String.format("[%s] Leader changed but not found the leader info in the packet. Re-sending the same packet to the server %s with the seqNum as %d.", config.getLocal().toString(), config.getLeader().toString(), offset));
                                        }
                                    } else if (packet.getStatus() == Constants.RESPONSE_STATUS.ELECTION.ordinal()) {
                                        logger.info(String.format("[%s] Leader changed. Election process going on. Sleeping for %d time before retrying for the seqNum %d.", config.getLocal().toString(), Constants.PRODUCER_SLEEP_TIME, offset));
                                        try {
                                            Thread.sleep(Constants.PRODUCER_SLEEP_TIME);
                                        } catch (InterruptedException e) {
                                            logger.error(String.format("[%s] Error while sleeping for %d amount of time.", config.getLocal().toString(), Constants.PRODUCER_SLEEP_TIME), e);
                                        }
                                    } else {
                                        logger.warn(String.format("[%s] Received invalid response status %d from the server %s.", config.getLocal().toString(), packet.getStatus(), config.getLeader().toString()));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    logger.warn(String.format("[%s] Timeout happen and client haven't received the acknowledgement of the packet with the sequence number %d. Retrying.", config.getLocal().toString(), offset));
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s] Unable to open the file at the location %s.", config.getLocal().toString(), config.getLocation()), exception);
        }
    }
}
