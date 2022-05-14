package application.controllers;

import application.configuration.Config;
import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import consensus.controllers.Channels;
import models.Header;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.Logger;
import utils.JSONDeserializer;
import utils.PacketHandler;

public abstract class Client {
    protected Logger logger;
    protected Config config;
    protected Host broker;
    protected int brokerNum;

    public Client(Logger logger, Config config) {
        this.logger = logger;
        this.config = config;
        brokerNum = -1;
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
                                    logger.info(String.format("[%s] Leader changed as per broker %s. Sending data to the server %s with the sequence number %d.", config.getLocal().toString(), broker.toString(), packet.getObject().toString(), offset));
                                    Channels.remove(broker.toString());
                                    broker = packet.getObject();
                                } else {
                                    logger.warn(String.format("[%s] Leader changed but not found the leader info in the packet. Re-sending the same packet to the server %s with the seqNum as %d.", config.getLocal().toString(), broker.toString(), offset));
                                }
                            } else if (packet.getStatus() == Constants.RESPONSE_STATUS.ELECTION.ordinal()) {
                                logger.info(String.format("[%s] Leader changed. Election process going on. Sleeping for %d time before retrying for the seqNum %d.", config.getLocal().toString(), Constants.CLIENT_SLEEP_TIME, offset));
                                try {
                                    Thread.sleep(Constants.CLIENT_SLEEP_TIME);
                                } catch (InterruptedException e) {
                                    logger.error(String.format("[%s] Error while sleeping for %d amount of time.", config.getLocal().toString(), Constants.CLIENT_SLEEP_TIME), e);
                                }
                            } else {
                                logger.warn(String.format("[%s] Received invalid response status %d from the server %s.", config.getLocal().toString(), packet.getStatus(), broker.toString()));
                            }
                        }
                    }
                }
            } else {
                logger.info(String.format("[%s] Received the response with the sequence number: %d. Expected sequence number: %d from leader %s.", config.getLocal().toString(), header.getSeqNum(), offset, broker.toString()));
            }
        }

        return isSuccess;
    }

    /**
     * Set the broker
     */
    protected void setNewBroker() {
        brokerNum++;
        if (brokerNum == config.getMemberCount()) {
            brokerNum = 0;
        }
        broker = config.getMember(brokerNum);
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
