package consensus.controllers;

import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import consensus.models.AppendEntriesRequest;
import consensus.models.AppendEntriesResponse;
import consensus.models.VoteRequest;
import consensus.models.VoteResponse;
import controllers.Connection;
import controllers.NodeService;
import models.Header;
import models.Host;
import models.Packet;
import models.PullRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import stateMachine.Broker;
import utils.JSONDeserializer;
import utils.PacketHandler;

/**
 * Responsible for handling requests from other hosts.
 *
 * @author Palak Jain
 */
public class RequestHandler {
    private static final Logger logger = LogManager.getLogger(RequestHandler.class);
    private Connection connection;
    private NodeService nodeService;
    private Broadcast broadcast;
    private Replication replication;

    public RequestHandler(Connection connection) {
        this.connection = connection;
        nodeService = new NodeService();
        broadcast = new Broadcast();
        replication = new Replication();
    }

    /**
     * Calls appropriate handler to process the request based on who made the request.
     */
    public void process() {
        boolean running = true;
        ThreadContext.put("module", CacheManager.getLocal().getName());

        while (running && connection.isOpen()) {

            byte[] requestOrResponse = connection.receive();

            if (requestOrResponse != null) {
                Header.Content header = PacketHandler.getHeader(requestOrResponse);

                if (header != null) {
                    connection.setInfo(header.getSource().getAddress(), header.getSource().getPort());

                    if (header.getRequester() == Constants.REQUESTER.PRODUCER.ordinal()) {
                        processProducerRequest(header, requestOrResponse);
                    } else if (header.getRequester() == Constants.REQUESTER.SERVER.ordinal()) {
                        processServerRequest(header, requestOrResponse);
                    } else if (header.getRequester() == Constants.REQUESTER.CONSUMER.ordinal()) {
                        processConsumerRequest(connection.getDestination(), header, requestOrResponse);
                    }
                }
            } else {
                running = false;
            }
        }
    }

    /**
     * Process the request or response received from producer
     */
    private void processProducerRequest(Header.Content header, byte[] requestOrResponse) {
        if (header.getType() == Constants.HEADER_TYPE.DATA.ordinal()) {
            logger.info(String.format("[%s] Received DATA requestOrResponse from client: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));
            broadcast.process(connection, requestOrResponse, header.getSeqNum());
        } else {
            logger.info(String.format("[%s] Received invalid %d requestOrResponse type from client: %s.", CacheManager.getLocal().toString(), header.getType(), connection.getDestination().toString()));
            nodeService.sendNACK(connection.getDestination(), Constants.REQUESTER.SERVER, header.getSeqNum());
        }
    }

    /**
     * Process the request or response received from consumer
     */
    private void processConsumerRequest(Host consumer, Header.Content header, byte[] requestOrResponse) {
        if (header.getType() == Constants.HEADER_TYPE.PULL_REQ.ordinal()) {
            logger.info(String.format("[%s] Received PULL Request from consumer %s. ", CacheManager.getLocal().toString(), consumer.toString()));

            byte[] body = PacketHandler.getData(requestOrResponse);
            if (body != null) {
                Packet<PullRequest> packet = JSONDeserializer.deserializePacket(body, new TypeToken<Packet<PullRequest>>(){}.getType());
                if (packet != null && packet.getObject() != null) {
                    Broker.pull(consumer, packet.getObject(), header.getSeqNum());
                }
            }
        }
    }

    /**
     * Process the request or response received from another server
     */
    private void processServerRequest(Header.Content header, byte[] requestOrResponse) {
        byte[] body = PacketHandler.getData(requestOrResponse);

        if (body != null) {
            if (header.getType() == Constants.HEADER_TYPE.ENTRY_REQ.ordinal()) {
                logger.info(String.format("[%s] Received AppendEntry request packet from leader: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                Packet<AppendEntriesRequest> packet = JSONDeserializer.deserializePacket(body, new TypeToken<Packet<AppendEntriesRequest>>(){}.getType());
                if (packet != null && packet.getObject() != null) {
                    replication.appendEntries(packet.getObject());
                }
            } else if (header.getType() == Constants.HEADER_TYPE.ENTRY_RESP.ordinal()) {
                logger.info(String.format("[%s] Received AppendEntry response packet from follower: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                Packet<AppendEntriesResponse> packet = JSONDeserializer.deserializePacket(body, new TypeToken<Packet<AppendEntriesResponse>>(){}.getType());
                if (packet != null && packet.getObject() != null) {
                    replication.processAcknowledgement(packet.getObject());
                }
            } else if (header.getType() == Constants.HEADER_TYPE.VOTE_REQ.ordinal()) {
                logger.info(String.format("[%s] Received VoteRequest packet from candidate %s", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                Packet<VoteRequest> packet = JSONDeserializer.deserializePacket(body, new TypeToken<Packet<VoteRequest>>(){}.getType());
                if (packet != null && packet.getObject() != null) {
                    Election.processVoteRequest(packet.getObject());
                }
            } else if (header.getType() == Constants.HEADER_TYPE.VOTE_RESP.ordinal()) {
                logger.info(String.format("[%s] Received VoteResponse packet from voter %s", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                Packet<VoteResponse> packet = JSONDeserializer.deserializePacket(body, new TypeToken<Packet<VoteResponse>>(){}.getType());
                if (packet != null && packet.getObject() != null) {
                    Election.processVoteResponse(packet.getObject());
                }
            } else {
                logger.info(String.format("[%s] Received invalid %d header type from server: %s.", CacheManager.getLocal().toString(), header.getType(), connection.getDestination().toString()));
                nodeService.sendNACK(connection.getDestination(), Constants.REQUESTER.SERVER, header.getSeqNum());
            }
        }
    }
}
