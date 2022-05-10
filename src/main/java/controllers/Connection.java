package controllers;

import models.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import consensus.controllers.CacheManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Responsible for sending and receiving data from the channel being established between two hosts.
 *
 * @author Palak Jain
 */
public class Connection implements Sender, Receiver {
    private static final Logger logger = LogManager.getLogger(Connection.class);
    private Socket channel;
    protected Host destination;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private volatile boolean isClosed;

    public Connection(Socket channel, String destinationIPAddress, int destinationPort) {
        this.channel = channel;
        destination = new Host(destinationIPAddress, destinationPort);
    }

    public Connection() {}

    /**
     * Opens the connection with another host by opening the input stream or output stream based on the requirement
     * @return true if successful else false
     */
    public boolean openConnection() {
        boolean isSuccess = false;

        try {
            inputStream = new DataInputStream(channel.getInputStream());
            outputStream = new DataOutputStream(channel.getOutputStream());
            isSuccess = true;
        } catch (IOException exception) {
            System.err.printf("[%s] Unable to get input/output stream. Error: %s.\n", CacheManager.getLocal().toString(), exception.getMessage());
        }

        return isSuccess;
    }

    /**
     * Set the address and port of the host from whom received OR to whom sent the packet
     */
    public void setInfo(String address, int port) {
        destination = new Host(address, port);
    }

    /**
     * Get the address and port of the host from whom received OR to whom sent the packet
     */
    public Host getDestination() {
        return destination;
    }

    /**
     * @return true is socket is still open else false
     */
    public boolean isOpen() {
        return !isClosed;
    }

    /**
     * Set the timeout period on channel read operation
     */
    public void setTimer(int timeout) {
        try {
            channel.setSoTimeout(timeout);
        } catch (SocketException e) {
            logger.error(String.format("[%s] Unable to set wait timer of amount %d. Error: ", CacheManager.getLocal().toString(), timeout), e);
        }
    }

    /**
     * Reset the timeout period on channel read operation
     */
    public void resetTimer() {
        try {
            channel.setSoTimeout(0);
        } catch (SocketException e) {
            logger.error(String.format("[%s] Unable to reset wait timer to 0. Error: ", CacheManager.getLocal().toString()), e);
        }
    }

    /**
     * Sending message to another host
     * @param message
     * @return true if successful else false
     */
    @Override
    public boolean send(byte[] message) {
        boolean isSend = false;

        try {
            outputStream.writeInt(message.length);
            outputStream.write(message);
            isSend = true;
        } catch (SocketException exception) {
            //If getting socket exception means connection is refused or cancelled. In this case, will not attempt to make any operation
            isClosed = true;
        } catch (IOException exception) {
            System.err.printf("[%s] Fail to send message to %s. Error: %s.\n", CacheManager.getLocal().toString(), destination.toString(), exception.getMessage());
        }

        return isSend;
    }

    /**
     * Receive message from another host.
     * @return the message being received
     */
    @Override
    public byte[] receive() {
        byte[] buffer = null;

        try {
            int length = inputStream.readInt();
            if(length > 0) {
                buffer = new byte[length];
                inputStream.readFully(buffer, 0, buffer.length);
            }
        } catch (EOFException | SocketTimeoutException ignored) {} //No more content available to read
        catch (SocketException exception) {
            //If getting socket exception means connection is refused or cancelled. In this case, will not attempt to make any operation
            isClosed = true;
        } catch (IOException exception) {
            System.err.printf("[%s] Fail to receive message from %s. Error: %s.\n", CacheManager.getLocal().toString(), destination.toString(), exception.getMessage());
        }

        return buffer;
    }

    /**
     * Finds out if there are some bytes to read from the incoming channel
     */
    public boolean isAvailable() {
        boolean isAvailable = false;

        try {
            isAvailable = inputStream.available() != 0;
        } catch (IOException e) {
            System.err.printf("Unable to get the available bytes to read. Error: %s", e.getMessage());
        }

        return isAvailable;
    }

    /**
     * CLose the connection between two hosts
     */
    public void closeConnection() {
        try {
            if (inputStream != null) inputStream.close();
            if (outputStream != null) outputStream.close();
            if (channel != null) channel.close();

            isClosed = true;
        } catch (IOException e) {
            System.err.printf("[%s] Unable to close the connection. Error: %s", CacheManager.getLocal().toString(), e.getMessage());
        }
    }
}
