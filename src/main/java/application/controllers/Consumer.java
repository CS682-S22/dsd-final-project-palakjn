package application.controllers;

import application.configuration.Config;
import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import models.Packet;
import models.PullRequest;
import models.PullResponse;
import org.apache.logging.log4j.LogManager;
import utils.FileManager;
import utils.JSONDeserializer;
import utils.PacketHandler;
import utils.Strings;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Periodically send PULL request to pull n number of logs from the arbitrary offset
 *
 * @author Palak Jain
 */
public class Consumer extends Client {

    public Consumer(Config config) {
        super(LogManager.getLogger(Consumer.class), config);
    }

    @Override
    public void send() {
        //Do nothing
    }

    /**
     * Pulling n number of logs from the arbitrary offset from the leader
     */
    @Override
    public void pull() {
        Scanner input = new Scanner(System.in);

        System.out.print("Enter to send new pull request to leader (Enter 'exit' to exit): ");
        String output =  input.nextLine();

        while (!Strings.isNullOrEmpty(output)) {
            logger.info(String.format("[%s] Getting %d logs from leader %s from the offset %d.", config.getLocal().toString(), config.getNumOfLogs(), config.getLeader().toString(), config.getOffset()));
            PullRequest pullRequest = new PullRequest(config.getOffset(), config.getNumOfLogs());
            Packet<PullRequest> packet = new Packet<>(Constants.RESPONSE_STATUS.OK.ordinal(), pullRequest);
            byte[] data = PacketHandler.createPacket(Constants.REQUESTER.CONSUMER, Constants.HEADER_TYPE.PULL_REQ, packet, config.getOffset(), config.getLeader());
            config.getLeader().send(data);

            byte[] response = null;

            try {
                response = responseQueue.poll(Constants.CONSUMER_WAIT_TIME, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.error(String.format("[%s] Unable to pull the response received from the leader from the queue.", config.getLocal().toString()), e);
            }

            if (response != null) {
                if (processResponse(response, config.getOffset())) {
                    byte[] body = PacketHandler.getData(response);

                    if (body != null) {
                        Packet<PullResponse> pullResponsePacket = JSONDeserializer.deserializePacket(body, new TypeToken<Packet<PullResponse>>() {}.getType());

                        if (pullResponsePacket != null && pullResponsePacket.getObject() != null) {
                            PullResponse pullResponse = pullResponsePacket.getObject();

                            logger.info(String.format("[%s] Received %d number of logs [Expected %d] from the leader %s with nextOffset as %d.", config.getLocal().toString(), pullResponse.getNumOfLogs(), config.getNumOfLogs(), config.getLeader().toString(), pullResponse.getNextOffset()));

                            int index = 0;
                            while (index < pullResponse.getNumOfLogs()) {
                                byte[] content = pullResponse.getData(index);
                                FileManager.write(config.getLocation(), content);

                                index++;
                            }

                            config.setOffset(pullResponse.getNextOffset());
                        } else {
                            logger.warn(String.format("[%s] Either unable to parse received pull response or pull response object is null. Resending packet", config.getLocal().toString()));
                        }
                    }
                }
            } else {
                logger.warn(String.format("[%s] Timeout happen and consumer haven't received the packet with the sequence number %d. Retrying.", config.getLocal().toString(), config.getOffset()));
            }

            System.out.print("Enter to send new pull request to leader (Enter 'exit' to exit): ");
            output =  input.nextLine();
        }
    }
}
