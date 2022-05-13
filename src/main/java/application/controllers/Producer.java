package application.controllers;

import application.configuration.Config;
import configuration.Constants;
import org.apache.logging.log4j.LogManager;
import utils.PacketHandler;
import utils.Strings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for reading logs from the file and sending to the leader of the system.
 *
 * @author Palak Jain
 */
public class Producer extends Client {

    public Producer(Config config) {
        super(LogManager.getLogger(Producer.class), config);
    }

    /**
     * Read the log line by line and send it to the leader of the consensus system
     */
    @Override
    public void send() {
        try (BufferedReader reader = new BufferedReader(new FileReader(config.getLocation()))) {
            String line = reader.readLine();
            int offset = 0;

            while (!Strings.isNullOrEmpty(line)) {
                byte[] data = PacketHandler.createPacket(Constants.REQUESTER.PRODUCER, Constants.HEADER_TYPE.DATA, line.getBytes(StandardCharsets.UTF_8), offset, config.getLeader());
                logger.info(String.format("[%s] Sending %d bytes of data to the leader of the system %s.", config.getLocal().toString(), data.length, config.getLeader().toString()));
                config.getLeader().send(data);

                byte[] response = null;

                try {
                    response = responseQueue.poll(Constants.PRODUCER_WAIT_TIME, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error(String.format("[%s] Unable to pull the response received from the leader from the queue.", config.getLocal().toString()), e);
                }

                if (response != null) {
                    if (processResponse(response, offset)) {
                        offset += line.length();
                        line = reader.readLine();
                    }
                } else {
                    logger.warn(String.format("[%s] Timeout happen and client haven't received the acknowledgement of the packet with the sequence number %d. Retrying.", config.getLocal().toString(), offset));
                }
            }
        } catch (IOException exception) {
            logger.error(String.format("[%s] Unable to open the file at the location %s.", config.getLocal().toString(), config.getLocation()), exception);
        }
    }

    @Override
    public void pull() {
        //Do nothing
    }
}
