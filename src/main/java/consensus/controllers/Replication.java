package consensus.controllers;

import configuration.Constants;
import consensus.models.AppendEntries;
import consensus.models.Entry;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.FileManager;
import utils.PacketHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Periodically send the new logs if any to all the followers if the server is the leader
 *
 * @author Palak Jain
 */
public class Replication {
    private static final Logger logger = LogManager.getLogger(Replication.class);
    private static Timer timer;

    /**
     * Start the timer to replicate the logs to followers if the current server is the leader
     */
    public static void start() {
        timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal()) {
                    List<Host> followers = CacheManager.getNeighbors();
                    logger.info(String.format("[%s] Sending AppendEntry packet to %d followers.", CacheManager.getLocal().toString(), followers.size()));

                    for (Host follower : followers) {
                        replicate(follower);
                    }

                    restart();
                } else {
                    logger.info(String.format("[%s] Server is not anymore leader. Stopping replicating logs to followers. Current role: %s", CacheManager.getLocal().toString(), Constants.ROLE.values()[CacheManager.getCurrentRole()].name()));
                    stop();
                }
            }
        };

        timer.schedule(task, Constants.REPLICATION_PERIOD);
    }

    /**
     * Restart the timer
     */
    public static void restart() {
        stop();
        start();
    }

    /**
     * Stop the timer
     */
    public static void stop() {
        timer.cancel();
    }

    /**
     * Replicate the logs which are not sent before to the follower
     */
    private static void replicate(Host follower) {

        //Getting the length of the logs already being sent to the follower before
        int prefixLen = CacheManager.getSentLength(follower.getId());

        //Getting batch of 10 logs metadata from the prefixLen
        List<Entry> entries = CacheManager.getEntries(prefixLen);

        int prefixTerm = 0;

        if (prefixLen > 0) {
            prefixTerm = CacheManager.getEntry(prefixLen - 1).getTerm();
        }

        List<byte[]> suffix = new ArrayList<>();

        for (Entry entry : entries) {
            byte[] data = FileManager.read(entry.getFromOffset(), (entry.getToOffset() + 1) - entry.getFromOffset());

            if (data != null) {
                suffix.add(data);
            } else {
                break;
            }
        }

        AppendEntries appendEntries = new AppendEntries(CacheManager.getLocal().getId(), CacheManager.getTerm(), prefixLen, prefixTerm, CacheManager.getCommitLength(), suffix);
        Packet<AppendEntries> packet = new Packet<>(Constants.PACKET_TYPE.APPEND_ENTRIES.ordinal(), CacheManager.getTerm(), Constants.RESPONSE_STATUS.OK.ordinal(), appendEntries);

        follower.send(PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.REQ, packet, 0, follower));
        logger.info(String.format("[%s] Send AppendEntries packet to the follower %s with %d new logs [PrefixLen: %d. PrefixTerm: %d].", CacheManager.getLocal().toString(), follower.toString(), suffix.size(), prefixLen, prefixTerm));
    }
}
