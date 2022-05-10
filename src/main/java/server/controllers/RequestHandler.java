package server.controllers;

import configuration.Constants;
import controllers.Connection;
import controllers.NodeService;
import models.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
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
    private String name;

    public RequestHandler(String name, Connection connection) {
        this.connection = connection;
        nodeService = new NodeService();
        broadcast = new Broadcast();
        this.name = name;
    }

    /**
     * Calls appropriate handler to process the request based on who made the request.
     */
    public void process() {
        boolean running = true;
        ThreadContext.put("module", name);

        while (running && connection.isOpen()) {

            byte[] request = connection.receive();

            if (request != null) {
                Header.Content header = PacketHandler.getHeader(request);

                if (header != null) {
                    connection.setInfo(header.getSource().getAddress(), header.getSource().getPort());

                    if (header.getRequester() == Constants.REQUESTER.CLIENT.ordinal()) {
                        if (header.getType() == Constants.HEADER_TYPE.DATA.ordinal()) {
                            logger.info(String.format("[%s] Received DATA request from client: %s.", CacheManager.getLocal().toString(), connection.getDestination().toString()));
                            broadcast.process(connection, request, header.getSeqNum());
                        } else {
                            logger.info(String.format("[%s] Received invalid %d request type from client: %s.", CacheManager.getLocal().toString(), header.getType(), connection.getDestination().toString()));
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
