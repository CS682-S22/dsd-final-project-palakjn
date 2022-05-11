package consensus.controllers;

import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import consensus.models.AppendEntriesRequest;
import consensus.models.AppendEntriesResponse;
import controllers.Connection;
import controllers.NodeService;
import models.Header;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import utils.JSONDesrializer;
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

                    if (header.getRequester() == Constants.REQUESTER.CLIENT.ordinal()) {
                        if (header.getType() == Constants.HEADER_TYPE.DATA.ordinal()) {
                            logger.info(String.format("[%s] Received DATA requestOrResponse from client: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));
                            broadcast.process(connection, requestOrResponse, header.getSeqNum());
                        } else {
                            logger.info(String.format("[%s] Received invalid %d requestOrResponse type from client: %s.", CacheManager.getLocal().toString(), header.getType(), connection.getDestination().toString()));
                            nodeService.sendNACK(connection, Constants.REQUESTER.SERVER, header.getSeqNum());
                        }
                    } else if (header.getRequester() == Constants.REQUESTER.SERVER.ordinal()) {
                        byte[] body = PacketHandler.getData(requestOrResponse);

                        if (body != null) {
                            if (header.getType() == Constants.HEADER_TYPE.ENTRY_REQ.ordinal()) {
                                logger.info(String.format("[%s] Received AppendEntry request packet from leader: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                                Packet<AppendEntriesRequest> packet = JSONDesrializer.deserializePacket(body, new TypeToken<Packet<AppendEntriesRequest>>(){}.getType());
                                if (packet != null) {
                                    replication.appendEntries(packet.getObject());
                                }
                            } else if (header.getType() == Constants.HEADER_TYPE.ENTRY_RESP.ordinal()) {
                                logger.info(String.format("[%s] Received AppendEntry response packet from follower: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                                Packet<AppendEntriesResponse> packet = JSONDesrializer.deserializePacket(body, new TypeToken<Packet<AppendEntriesResponse>>(){}.getType());
                                if (packet != null) {
                                    replication.processAcknowledgement(packet.getObject());
                                }
                            } else if (header.getType() == Constants.HEADER_TYPE.VOTE_REQ.ordinal()) {

                            } else if (header.getType() == Constants.HEADER_TYPE.VOTE_RESP.ordinal()) {

                            } else {
                                logger.info(String.format("[%s] Received invalid %d header type from server: %s.", CacheManager.getLocal().toString(), header.getType(), connection.getDestination().toString()));
                                nodeService.sendNACK(connection, Constants.REQUESTER.SERVER, header.getSeqNum());
                            }
                        }
                    }
                }
            } else {
                running = false;
            }
        }
    }
}
