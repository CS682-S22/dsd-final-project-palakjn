import application.configuration.Config;
import application.controllers.Client;
import application.controllers.Consumer;
import application.controllers.Producer;
import consensus.controllers.CacheManager;
import org.apache.logging.log4j.ThreadContext;
import utils.JSONDeserializer;
import utils.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * An application to send logs to the consumer of the system.
 *
 * @author Palak Jain
 */
public class Application {
    private Client client;

    public Application() {
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
}
