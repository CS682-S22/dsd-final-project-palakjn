import application.configuration.Config;
import application.controllers.Client;
import application.controllers.Consumer;
import application.controllers.Producer;
import configuration.Constants;
import controllers.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import consensus.controllers.CacheManager;
import utils.JSONDeserializer;
import utils.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An application to send logs to the consumer of the system.
 *
 * @author Palak Jain
 */
public class Application {
    private static final Logger logger = LogManager.getLogger(Application.class);
    private Client client;
    private ExecutorService threadPool;
    private boolean running;

    public Application() {
        threadPool = Executors.newFixedThreadPool(Constants.NUM_OF_THREADS);
        running = true;
    }

    public static void main(String[] args) {
        Application application = new Application();
        String location = application.getConfigLocation(args);

        if (!Strings.isNullOrEmpty(location)) {
            Config config = application.getConfig(location);

            if (application.isValid(config)) {
                ThreadContext.put("module", config.getLocal().getName());
                CacheManager.setLocal(config.getLocal());
                if (config.isProducer()) {
                    application.client = new Producer(config);
                } else if (config.isConsumer()) {
                    application.client = new Consumer(config);
                }

                //Joining to the network
                logger.info(String.format("[%s] Listening on port %d.", config.getLocal().getAddress(), config.getLocal().getPort()));
                System.out.printf("[%s] Listening on port %d.\n", config.getLocal().getAddress(), config.getLocal().getPort());

                //Starting thread to listen for the connections to get
                Thread connectionThread = new Thread(() -> application.listen(config));
                connectionThread.start();

                if (config.isProducer()) {
                    application.client.send();
                } else if (config.isConsumer()) {
                    application.client.pull();
                }
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
     * Listen for new connections from consumer/consensus system to receive response packet
     */
    private void listen(Config config) {
        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(config.getLocal().getPort());
        } catch (IOException exception) {
            logger.error(String.format("Fail to start the producer at the node %s: %d.", config.getLocal().getAddress(), config.getLocal().getPort()), exception);
            return;
        }

        while (running) {
            try {
                ThreadContext.put("module", config.getLocal().getName());
                Socket socket = serverSocket.accept();
                logger.debug(String.format("[%s] Received the connection from server.", config.getLocal().toString()));
                Connection connection = new Connection(socket, socket.getInetAddress().getHostAddress(), socket.getPort());
                if (connection.openConnection()) {
                    threadPool.execute(() -> client.listenForResponse(connection));
                }
            } catch (IOException exception) {
                logger.error(String.format("[%s:%d] Fail to accept the connection from another host. ", config.getLocal().getAddress(), config.getLocal().getPort()), exception);
            }
        }
    }
}
