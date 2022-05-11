package consensus.controllers;

import configuration.Constants;
import consensus.models.AppendEntriesRequest;
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
    private String name;

    public RequestHandler(String name, Connection connection) {
        this.connection = connection;
        nodeService = new NodeService();
        broadcast = new Broadcast();
        replication = new Replication();
        this.name = name;
    }

    /**
     * Calls appropriate handler to process the request based on who made the request.
     */
    public void process() {
        boolean running = true;
        ThreadContext.put("module", name);

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
                            nodeService.sendNACK(connection, Constants.REQUESTER.SERVER, header.getSeqNum(), connection.getDestination());
                        }
                    } else if (header.getRequester() == Constants.REQUESTER.SERVER.ordinal()) {
                        if (header.getType() == Constants.HEADER_TYPE.REQ.ordinal()) {
                            logger.info(String.format("[%s] Received REG requestOrResponse from server: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                            byte[] body = PacketHandler.getData(requestOrResponse);

                            if (body != null) {
                                Packet<?> packet = JSONDesrializer.fromJson(body, Packet.class);

                                if (packet != null) {
                                    if (packet.getType() == Constants.PACKET_TYPE.APPEND_ENTRIES.ordinal() && packet.getObject() instanceof AppendEntriesRequest appendEntriesRequest) {
                                        replication.appendEntries(appendEntriesRequest);
                                    } else if (packet.getType() == Constants.PACKET_TYPE.VOTE.ordinal()) {

                                    }
                                }
                            }
                        } else if (header.getType() == Constants.HEADER_TYPE.RESP.ordinal()) {
                            logger.info(String.format("[%s] Received RESP response from server: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));

                            byte[] body = PacketHandler.getData(requestOrResponse);

                            if (body != null) {
                                Packet<?> packet = JSONDesrializer.fromJson(body, Packet.class);

                                if (packet != null) {
                                    if (packet.getType() == Constants.PACKET_TYPE.APPEND_ENTRIES.ordinal()) {

                                    } else if (packet.getType() == Constants.PACKET_TYPE.VOTE.ordinal()) {

                                    }
                                }
                            }
                        } else {
                            logger.info(String.format("[%s] Received invalid %d requestOrResponse type from server: %s.", CacheManager.getLocal().toString(), header.getType(), connection.getDestination().toString()));
                            nodeService.sendNACK(connection, Constants.REQUESTER.SERVER, header.getSeqNum(), connection.getDestination());
                        }
                    }
                }
            } else {
                running = false;
            }
        }
    }
}
