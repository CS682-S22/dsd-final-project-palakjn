package consensus.controllers;

import controllers.Connection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Responsible for adding new channel and reusing existing channels
 *
 * @author Palak Jain
 */
public class Channels {
    private static Map<String, Connection> connections = new HashMap<>();
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Add new channel
     */
    public static void add(String key, Connection connection) {
        lock.writeLock().lock();

        if (!connections.containsKey(key)) {
            connections.put(key, connection);
        }

        lock.writeLock().unlock();
    }

    /**
     * Get the existing connection if exist
     */
    public static Connection get(String key) {
        Connection connection;
        lock.readLock().lock();

        connection = connections.getOrDefault(key, null);

        lock.readLock().unlock();
        return connection;
    }

    /**
     * Add new channel if exist or update existing one
     */
    public static void upsert(String key, Connection connection) {
        lock.writeLock().lock();

        connections.put(key, connection);

        lock.writeLock().unlock();
    }

    /**
     * CLose and remove the connection channel
     */
    public static void remove(String key) {
        lock.writeLock().lock();

        Connection connection = connections.getOrDefault(key, null);
        if (connection != null) {
            connection.closeConnection();
            connections.remove(key);
        }

        lock.writeLock().unlock();
    }
}
