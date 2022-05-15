package models;

import controllers.Connection;
import controllers.LossyConnection;

import java.net.Socket;

/**
 * Responsible for creating new connection between two hosts.
 * It can create two types of connections:
 *      1) Default connection: Messages may lose because of network issues
 *      2) Lossy connection: Few messages can lose or messages will be delayed
 *
 * @author Palak Jain
 */
public class Connections {
    private static float lossyRate;
    private static int delay;

    private Connections() {}

    /**
     * Initialize with lossy rate and delay set as config
     */
    public static void init(float lossyRate, int delay) {
        Connections.lossyRate = lossyRate;
        Connections.delay = delay;
    }

    /**
     * Get the type of connection object based on delay and loss rate set by the use
     */
    public static Connection getConnection(Socket channel, String address, int port) {
        Connection connection;

        if(delay > 0 || lossyRate > 0) {
            connection = new LossyConnection(channel, lossyRate, delay, address, port);
        } else {
            connection = new Connection(channel, address, port);
        }

        return connection;
    }
}
