package models;

import com.google.gson.annotations.Expose;
import controllers.Connection;
import controllers.NodeService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import configuration.Constants;
import consensus.controllers.CacheManager;
import consensus.controllers.Channels;
import utils.Strings;

/**
 * Responsible for holding host information
 *
 * @author Palak Jain
 */
public class Host {
    @Expose
    private int id;
    @Expose
    private String address;
    @Expose
    private int port;
    private NodeService nodeService;
    private final Logger logger = LogManager.getLogger(Host.class);

    public Host(int id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
        this.nodeService = new NodeService();
    }

    public Host(String address, int port) {
        this.address = address;
        this.port = port;
        this.nodeService = new NodeService();
    }

    public Host() {
        this.nodeService = new NodeService();
    }

    /**
     * Get the identifier of the host
     */
    public int getId() {
        return id;
    }

    /**
     * Set the identifier of the host
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Get the address of the host
     */
    public String getAddress() {
        return address;
    }

    /**
     * Set the address of the host
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Get the port of the host
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the port of the host
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Send the packet to the server Return true if success else false [No wait for acknowledgement]
     */
    public boolean send(byte[] data) {
        boolean isSuccess = false;

        Connection connection = getConnection();

        if (connection != null) {
            isSuccess = connection.send(data);

            if (isSuccess) {
                logger.info(String.format("[%s] Send %d bytes of data to the broker %s:%d.", CacheManager.getLocal().toString(), data.length, address, port));
            }
        }

        return isSuccess;
    }

    /**
     * CLose the connection with the server
     */
    public void close() {
       Channels.remove(toString());
    }

    @Override
    public String toString() {
        return String.format("%s:%d", address, port);
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof Host host) {
            isEqual = host.getId() == id && host.getAddress().equals(address) && host.getPort() == port;
        }

        return isEqual;
    }

    /**
     * Verifies if the object is valid
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(address) && port >= Constants.START_VALID_PORT && port <= Constants.END_VALID_PORT;
    }

    /**
     * Get the connection object
     */
    private Connection getConnection() {
        Connection connection = Channels.get(toString());

        if (connection == null || !connection.isOpen()) {
            connection = nodeService.connect(address, port);
        }

        if (connection == null || !connection.isOpen()) {
            connection = null;
        } else {
            Channels.upsert(toString(), connection);
        }

        return connection;
    }
}
