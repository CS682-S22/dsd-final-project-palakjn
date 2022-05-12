package consensus.controllers;

import configuration.Constants;
import consensus.models.VoteRequest;
import consensus.models.VoteResponse;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import utils.PacketHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Responsible for starting an election when detected the failure
 *
 * @author Palak Jain
 */
public class Election {
    private static final Logger logger = LogManager.getLogger(Election.class);
    private static final List<Integer> votesReceived = new ArrayList<>();
    private static Timer electionTimer;
    private static volatile boolean isElectionTimerStarted;

    /**
     * Start the election when detected that leader is failed.
     */
    public static void startElection() {
        FaultDetector.stopTimer();

        if (CacheManager.getCurrentRole() != Constants.ROLE.CANDIDATE.ordinal()) {
            logger.info(String.format("[%s] Starting election process.", CacheManager.getLocal().toString()));

            int currentTerm = CacheManager.incrementTerm();
            CacheManager.setCurrentRole(Constants.ROLE.CANDIDATE.ordinal());
            CacheManager.setVoteFor(CacheManager.getLocal().getId());
            votesReceived.clear();
            votesReceived.add(CacheManager.getLocal().getId());

            int lastTerm = CacheManager.getLastTerm();

            VoteRequest voteRequest = new VoteRequest(CacheManager.getLocal().getId(), currentTerm, CacheManager.getLogLength(), lastTerm);
            Packet<VoteRequest> packet = new Packet<>(Constants.RESPONSE_STATUS.OK.ordinal(), voteRequest);

            List<Host> nodes = CacheManager.getNeighbors();

            for (Host node : nodes) {
                byte[] request = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.VOTE_REQ, packet, 0, node);
                node.send(request);
                logger.info(String.format("[%s] Sending vote request to voter %s with the current term %d.", CacheManager.getLocal().toString(), node.toString(), currentTerm));
            }

            startTimer();
        } else {
            logger.info(String.format("[%s] Server is already in candidate state. Not Starting the election.", CacheManager.getLocal().toString()));
        }
    }

    /**
     * Processing Vote Request received from candidate
     */
    public static synchronized void processVoteRequest(VoteRequest voteRequest) {
        int currentTerm = CacheManager.getTerm();
        Host candidate = CacheManager.getNeighbor(voteRequest.getCandidateId());

        if (voteRequest.getCandidateTerm() > currentTerm) {
            logger.info(String.format("[%s] Found voter %s's term value %d more than current term value %d. Changing role to Follower.", CacheManager.getLocal().toString(), candidate.toString(), voteRequest.getCandidateTerm(), currentTerm));
            currentTerm = CacheManager.setTerm(voteRequest.getCandidateTerm());
            CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
            CacheManager.setVoteFor(-1);

            FaultDetector.startTimer();
        }

        int lastTerm = CacheManager.getLastTerm();

        boolean logOk = (voteRequest.getCandidateLogTerm() > lastTerm) || (voteRequest.getCandidateLogTerm() == lastTerm && voteRequest.getCandidateLogLength() >= CacheManager.getLogLength());

        VoteResponse voteResponse;
        if (voteRequest.getCandidateTerm() == currentTerm && logOk && CacheManager.setVoteFor(voteRequest.getCandidateId())) {
            logger.info(String.format("[%s] Granting vote to candidate %s. [Candidate Term: %d, Voter's term: %d, CandidateLogTerm: %d, VoterLogTerm: %d, CandidateLogLength: %d, VoterLogLength: %d]",
                    CacheManager.getLocal().toString(), candidate.toString(), voteRequest.getCandidateTerm(), currentTerm, voteRequest.getCandidateLogTerm(), lastTerm, voteRequest.getCandidateLogLength(), CacheManager.getLogLength()));
            voteResponse = new VoteResponse(CacheManager.getLocal().getId(), currentTerm, true);
        } else {
            logger.info(String.format("[%s] Rejecting vote to candidate %s. [Candidate Term: %d, Voter's term: %d, CandidateLogTerm: %d, VoterLogTerm: %d, CandidateLogLength: %d, VoterLogLength: %d, VotedFor: %d]",
                    CacheManager.getLocal().toString(), candidate.toString(), voteRequest.getCandidateTerm(), currentTerm, voteRequest.getCandidateLogTerm(), lastTerm, voteRequest.getCandidateLogLength(),
                    CacheManager.getLogLength(), CacheManager.getVotedFor()));
            voteResponse = new VoteResponse(CacheManager.getLocal().getId(), currentTerm, false);
        }

        Packet<VoteResponse> packet = new Packet<>(Constants.RESPONSE_STATUS.OK.ordinal(), voteResponse);
        byte[] response = PacketHandler.createPacket(Constants.REQUESTER.SERVER, Constants.HEADER_TYPE.VOTE_RESP, packet, 0, candidate);
        candidate.send(response);
    }

    /**
     * Collecting votes from the voters
     */
    public static synchronized void processVoteResponse(VoteResponse voteResponse) {
        int currentTerm = CacheManager.getTerm();
        Host voter = CacheManager.getNeighbor(voteResponse.getNodeId());
        logger.info(String.format("[%s] Received vote response from voter %s at the term %d.", CacheManager.getLocal().toString(), voter.toString(), currentTerm));

        if (CacheManager.getCurrentRole() == Constants.ROLE.CANDIDATE.ordinal() && voteResponse.getTerm() == currentTerm && voteResponse.isSuccess()) {
            votesReceived.add(voter.getId());
            logger.info(String.format("[%s] Received GRANTED vote from voter %s", CacheManager.getLocal().toString(), voter.toString()));

            if (votesReceived.size() > Math.ceil((CacheManager.getMemberCount() + 1) / 2.0)) {
                logger.info(String.format("[%s] Received vote from majority of members. Electing itself as leader", CacheManager.getLocal().toString()));
                CacheManager.setCurrentRole(Constants.ROLE.LEADER.ordinal());
                CacheManager.setCurrentLeader(CacheManager.getLocal().getId());
                stopTimer();
                FaultDetector.stopTimer();

                List<Host> followers = CacheManager.getNeighbors();
                for (Host follower : followers) {
                    CacheManager.setSentLength(follower.getId(), CacheManager.getLogLength());
                    CacheManager.setAckedLength(follower.getId(), 0);
                }

                Replication.startTimer();
            }
        } else if (voteResponse.getTerm() > currentTerm) {
            logger.info(String.format("[%s] Voter %s's term %d is more than current term %d. Changing role to follower.", CacheManager.getLocal().toString(), voter.toString(), voteResponse.getTerm(), currentTerm));
            stopTimer();
            CacheManager.setTerm(voteResponse.getTerm());
            CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
            FaultDetector.startTimer();
            CacheManager.setVoteFor(-1);
        }
    }

    /**
     * Start the election timer
     */
    public static synchronized void startTimer() {
        if (!isElectionTimerStarted) {
            isElectionTimerStarted = true;

            electionTimer = new Timer();
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    ThreadContext.put("module", CacheManager.getLocal().getName());
                    this.cancel();
                    stopTimer();

                    if (CacheManager.getCurrentRole() == Constants.ROLE.LEADER.ordinal() || CacheManager.getCurrentRole() == Constants.ROLE.FOLLOWER.ordinal()) {
                        logger.info(String.format("[%s] Server role changed to %s. Stopping election timer.", CacheManager.getLocal().toString(), Constants.ROLE.values()[CacheManager.getCurrentRole()].name()));
                    } else {
                        logger.info(String.format("[%s] Election timeout and server still haven't received votes from quorum of voters. Starting election again.", CacheManager.getLocal().toString()));
                        startTimer();
                    }
                }
            };

            int randomPeriod = (int)Math.floor(Math.random()*(Constants.ELECTION_MAX_TIME-Constants.ELECTION_MIN_TIME+1)+Constants.ELECTION_MIN_TIME);
            electionTimer.schedule(timerTask, randomPeriod);
            logger.info(String.format("[%s] Started election timer with period %d.", CacheManager.getLocal().toString(), randomPeriod));
        }
    }

    /**
     * Stop the election timer
     */
    public static synchronized void stopTimer() {
        logger.info(String.format("[%s] Stopping election timer.", CacheManager.getLocal().toString()));
        isElectionTimerStarted = false;
        electionTimer.cancel();
    }
}
