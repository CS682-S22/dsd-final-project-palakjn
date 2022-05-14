package application.controllers;

import application.configuration.Config;
import configuration.Constants;
import consensus.controllers.Channels;
import controllers.Connection;
import org.apache.logging.log4j.LogManager;
import utils.PacketHandler;
import utils.Strings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

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
            Scanner input = new Scanner(System.in);

            while (!Strings.isNullOrEmpty(line)) {
                if (config.isPressEnterToSend()) {
                    System.out.print("Enter to send new log to the broker: ");
                    input.nextLine();
                }

                if (broker == null) {
                    setNewBroker();
                }

                byte[] data = PacketHandler.createPacket(Constants.REQUESTER.PRODUCER, Constants.HEADER_TYPE.DATA, line.getBytes(StandardCharsets.UTF_8), offset, broker);
                logger.info(String.format("[%s] Sending %d bytes of data to the leader of the system %s.", config.getLocal().toString(), data.length, broker.toString()));
                if (broker.send(data)) {
                    Connection connection = Channels.get(broker.toString());
                    connection.setTimer(Constants.PRODUCER_WAIT_TIME);
                    byte[] response = connection.receive();

                    if (response != null) {
                        if (processResponse(response, offset)) {
                            offset += line.length();
                            line = reader.readLine();
                        } else {
                            logger.debug(String.format("[%s] Re-sending same log again to the leader %s", config.getLocal().toString(), broker.toString()));
                        }
                    } else {
                        logger.warn(String.format("[%s] Timeout happen and client haven't received the acknowledgement of the packet with the sequence number %d. Might be leader failed. Sending request to another host.", config.getLocal().toString(), offset));
                        Channels.remove(broker.toString());
                        broker = null;
                    }
                } else {
                    Channels.remove(broker.toString());
                    broker = null;
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
