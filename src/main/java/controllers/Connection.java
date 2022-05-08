package controllers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.controllers.CacheManager;

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
    protected String destinationIPAddress;
    protected int destinationPort;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private volatile boolean isClosed;

    public Connection(Socket channel, String destinationIPAddress, int destinationPort) {
        this.channel = channel;
        this.destinationIPAddress = destinationIPAddress;
        this.destinationPort = destinationPort;
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
     * @return the destination host address
     */
    public String getDestinationIPAddress() {
        return destinationIPAddress;
    }

    /**
     * @return the destination port number
     */
    public int getDestinationPort() {
        return destinationPort;
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
            logger.error(String.format("[%s:%d] Unable to set wait timer of amount %d. Error: ", destinationIPAddress, destinationPort, timeout), e);
        }
    }

    /**
     * Reset the timeout period on channel read operation
     */
    public void resetTimer() {
        try {
            channel.setSoTimeout(0);
        } catch (SocketException e) {
            logger.error(String.format("[%s:%d] Unable to reset wait timer to 0. Error: ", destinationIPAddress, destinationPort), e);
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
            System.err.printf("[%s] Fail to send message to %s:%d. Error: %s.\n", CacheManager.getLocal().toString(), destinationIPAddress, destinationPort, exception.getMessage());
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
            System.err.printf("[%s] Fail to receive message from %s:%d. Error: %s.\n", CacheManager.getLocal().toString(), destinationIPAddress, destinationPort, exception.getMessage());
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