package server;

import controllers.Connection;
import models.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.configuration.Config;
import server.configuration.Constants;
import server.controllers.CacheManager;
import server.controllers.RequestHandler;
import utils.JSONDesrializer;
import utils.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    private static final Logger logger = LogManager.getLogger(Server.class);
    private ExecutorService threadPool;
    private boolean running;

    public Server() {
        threadPool = Executors.newFixedThreadPool(Constants.NUM_OF_THREADS);
        running = true;
    }

    public static void main(String[] args) {
        Server server = new Server();
        String location = server.getConfigLocation(args);

        if (!Strings.isNullOrEmpty(location)) {
            Config config = server.getConfig(location);

            if (server.isValid(config)) {
                CacheManager.setLocal(config.getLocal());
                server.addMembers(config);

                //Getting data from disk to get the details of status of the nodes before crash
                server.setNodeStatus(config);
                server.setNodeWithOldOffsets(config);

                //Joining to the network
                logger.info(String.format("[%s] Listening on DATA/SYNC port %d.", config.getLocal().getAddress(), config.getLocal().getPort()));
                System.out.printf("[%s] Listening on DATA/SYNC port %d.\n", config.getLocal().getAddress(), config.getLocal().getPort());

                //Starting thread to listen for the connections
                Thread connectionThread = new Thread(() -> server.listen(config.getLocal().getAddress(), config.getLocal().getPort()));
                connectionThread.start();
            }
        }
    }

    /**
     * Get the location of the config file from arguments
     */
    private String getConfigLocation(String[] args) {
        String location = null;

        if (args.length == 2 &&
                args[0].equalsIgnoreCase("-config") &&
                !Strings.isNullOrEmpty(args[1])) {
            location = args[1];
        } else {
            System.out.println("Invalid Arguments");
        }

        return location;
    }

    /**
     * Read and De-Serialize the config file from the given location
     */
    private Config getConfig(String location) {
        Config config = null;

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(location))){
            config = JSONDesrializer.fromJson(reader, Config.class);
        }
        catch (IOException ioException) {
            System.out.printf("Unable to open configuration file at location %s. %s. \n", location, ioException.getMessage());
        }

        return config;
    }

    /**
     * Validates whether the config contains the required values or not
     */
    private boolean isValid(Config config) {
        boolean flag = false;

        if (config == null) {
            System.out.println("No configuration found.");
        } else if (!config.isValid()) {
            System.out.println("Invalid values found in the configuration file.");
        } else {
            flag = true;
        }

        return flag;
    }

    /**
     * Add member details to the local cache
     */
    private void addMembers(Config config) {
        int index = 0;

        while (index < config.getNumOfMembers()) {
            Host member = config.getMember(index);
            if (member != null) {
                CacheManager.addMember(member);
            }
            index++;
        }
    }

    /**
     * Get the status of the node from disk and add it to the cache (To support Crash recovery)
     */
    private void setNodeStatus(Config config) {
        //TODO: Set the current role as follower
    }

    /**
     * Get the offsets being read before by the server if it has been crashed before
     */
    private void setNodeWithOldOffsets(Config config) {

    }

    /**
     * Listen for DATA/SYNC connection
     */
    private void listen(String address, int port) {
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException exception) {
            logger.error(String.format("Fail to start the broker at the node %s: %d.", address, port), exception);
            return;
        }

        while (running) {
            try {
                Socket socket = serverSocket.accept();
                logger.info(String.format("[%s:%d] Received the connection from the host.", socket.getInetAddress().getHostAddress(), socket.getPort()));
                Connection connection = new Connection(socket, socket.getInetAddress().getHostAddress(), socket.getPort());
                if (connection.openConnection()) {
                    RequestHandler requestHandler = new RequestHandler(connection);
                    threadPool.execute(requestHandler::process);
                }
            } catch (IOException exception) {
                logger.error(String.format("[%s:%d] Fail to accept the connection from another host. ", address, port), exception);
            }
        }
    }
}