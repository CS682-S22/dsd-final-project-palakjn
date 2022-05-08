package controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.controllers.CacheManager;

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

            connection = new Connection(socket, address, port);
            if (!connection.openConnection()) {
                connection = null;
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s] Fail to make connection with the server %s:%d.", CacheManager.getLocal().toString(), address, port), exception.getMessage());
        }

        return connection;
    }
}
