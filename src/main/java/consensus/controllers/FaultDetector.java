package consensus.controllers;

import configuration.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Stores the timestamp when the last time heard from the leader if the current role is follower.
 * Starts election if found the unexpected gap between heartbeat messages
 *
 * @author Palak Jain
 */
public class FaultDetector {
    private static final Logger logger = LogManager.getLogger(FaultDetector.class);
    private static volatile boolean isRunning;
    private static volatile long lastTimeSpan;
    private static Timer timer;

    /**
     * Start the timer to detect leader failure
     */
    public synchronized static void startTimer() {
        if(!isRunning) {
            isRunning = true;
            timer = new Timer();
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    stopTimer();

                    ThreadContext.put("module", CacheManager.getLocal().getName());
                    long now = System.currentTimeMillis();
                    long timeSinceLastHeartBeat = now - lastTimeSpan;

                    if (timeSinceLastHeartBeat >= Constants.HEARTBEAT_TIMEOUT_THRESHOLD) {
                        logger.info(String.format("[%s] Haven't heard from leader since %d. Threshold is %d. Starting election.", CacheManager.getLocal().toString(), timeSinceLastHeartBeat, Constants.HEARTBEAT_TIMEOUT_THRESHOLD));
                        Election.startElection();
                    } else {
                        startTimer();
                    }
                }
            };

            int randomPeriod = (int) Math.floor(Math.random() * (Constants.FAULT_DETECTOR_MAX_VALUE - Constants.FAULT_DETECTOR_MIN_VALUE + 1) + Constants.FAULT_DETECTOR_MIN_VALUE);
            timer.schedule(task, randomPeriod);
            logger.info(String.format("[%s] Started fault tolerant timer with period %d.", CacheManager.getLocal().toString(), randomPeriod));
        }
    }

    /**
     * Stop the timer
     */
    public synchronized static void stopTimer() {
        ThreadContext.put("module",CacheManager.getLocal().getName());
        logger.info(String.format("[%s] Stopping the fault detector.", CacheManager.getLocal().toString()));
        isRunning = false;
        timer.cancel();
    }

    /**
     * Reporting the timespan when received AppendEntries packet from the leader
     */
    public static void heartBeatReceived() {
        lastTimeSpan = System.currentTimeMillis();
        logger.info(String.format("[%s] Received AppendEntries from leader at time %d.", CacheManager.getLocal().toString(), lastTimeSpan));
    }
}
