import consensus.controllers.FaultDetector;
import consensus.controllers.Replication;
import controllers.Connection;
import models.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import consensus.configuration.Config;
import configuration.Constants;
import consensus.controllers.CacheManager;
import consensus.controllers.RequestHandler;
import consensus.controllers.database.DataSource;
import consensus.controllers.database.EntryDB;
import consensus.controllers.database.StateDB;
import consensus.models.Entry;
import consensus.models.NodeState;
import utils.FileManager;
import utils.JSONDeserializer;
import utils.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
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
                ThreadContext.put("module", config.getLocal().getName());
                CacheManager.setLocal(config.getLocal());
                server.addMembers(config);
                DataSource.init(config);
                FileManager.init(config.getLocation());

                //Getting data from disk to get the details of status of the nodes before crash
                server.setNodeStatus();
                server.setNodeWithOldOffsets();
                CacheManager.initSentAndAckLength();

                //Joining to the network
                logger.info(String.format("[%s] Listening on port %d.", config.getLocal().getAddress(), config.getLocal().getPort()));
                System.out.printf("[%s] Listening on port %d.\n", config.getLocal().getAddress(), config.getLocal().getPort());

                //Starting thread to listen for the connections
                Thread connectionThread = new Thread(() -> server.listen(config));
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
            config = JSONDeserializer.fromJson(reader, Config.class);
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
    private void setNodeStatus() {
        NodeState nodeState = StateDB.get();

        if (nodeState != null) {
            CacheManager.setNodeState(nodeState);
        }

        CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
        FaultDetector.startTimer();
    }

    /**
     * Get the offsets being read before by the server if it has been crashed before
     */
    private void setNodeWithOldOffsets() {
        List<Entry> entries = EntryDB.getFrom(0);

        if (entries != null) {
            for (Entry entry : entries) {
                CacheManager.addEntry(entry);
            }
        }
    }

    /**
     * Listen for the connections from other servers/client
     */
    private void listen(Config config) {
        ThreadContext.put("module", config.getLocal().getName());
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(config.getLocal().getPort());
        } catch (IOException exception) {
            logger.error(String.format("Fail to start the broker at the node %s: %d.", config.getLocal().getAddress(), config.getLocal().getPort()), exception);
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
                logger.error(String.format("[%s:%d] Fail to accept the connection from another host. ", config.getLocal().getAddress(), config.getLocal().getPort()), exception);
            }
        }
    }
}
