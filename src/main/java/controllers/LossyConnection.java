package controllers;

import consensus.controllers.CacheManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;
import java.util.Random;

/**
 * Responsible for handling connections between hosts when there can be a possibility of data being lost or delayed
 *
 * @author Palak Jain
 */
public class LossyConnection extends Connection {
    private static final Logger logger = LogManager.getLogger(LossyConnection.class);

    private Random random;
    private int delayInMS;
    private float lossRate;

    public LossyConnection(Socket channel, float lossRate, int delayInMS, String destinationIPAddress, int destinationPort) {
        super(channel, destinationIPAddress, destinationPort);
        this.lossRate = lossRate;
        this.delayInMS = delayInMS;
        this.random = new Random();
    }

    /**
     * Sends the message to another host.
     *
     * Will sleep for the mentioned time.
     * Will send the mentioned number of messages to another host in order to loss few messages
     * @param message message to send to another host
     * @return true if successful or false
     */
    @Override
    public boolean send(byte[] message) {
        boolean isSend = false;

        boolean toFailOrDelay = toFailOrDelay();

        if (delayInMS != 0) {
            if (toFailOrDelay) {
                //Delaying only
                logger.info(String.format("[%s] Sleeping for %d time for delaying the message.", CacheManager.getLocal().toString(), delayInMS));
                try {
                    Thread.sleep(delayInMS);
                } catch (InterruptedException e) {
                    logger.error(String.format("Error while sleeping for %d milliseconds. Error:.", delayInMS), e);
                }
            }

            isSend = super.send(message);
        } else if (!toFailOrDelay) {
            isSend = super.send(message);
        } else {
            logger.warn(String.format("[%s] Packet being lost.", CacheManager.getLocal().toString()));
        }

        return isSend;
    }

    /**
     * Identifies whether to fail or delay the packet or not based on the loss rate
     * @return true if to fail or delay else false
     */
    private boolean toFailOrDelay() {
        //Referred Solution 2 of Project0 for the logic
        //https://github.com/CS682-S22/project-0-solutions/tree/c9af5497b7d65eccb41497878a019fe67389e737/solution-2
        int randNum = random.nextInt(100);
        return randNum < (lossRate * 100);
    }
}