package server.controllers;

import controllers.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Responsible for handling requests from other hosts.
 *
 * @author Palak Jain
 */
public class RequestHandler {
    private static final Logger logger = LogManager.getLogger(RequestHandler.class);
    private Connection connection;

    public RequestHandler(Connection connection) {
        this.connection = connection;
    }

    /**
     * Calls appropriate handler to process the request based on who made the request.
     */
    public void process() {
        boolean running = true;

        while (running && connection.isOpen()) {
            byte[] request = connection.receive();

            if (request != null) {

            } else {
                running = false;
            }
        }
    }
}
