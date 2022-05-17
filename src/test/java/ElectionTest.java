import com.google.gson.reflect.TypeToken;
import configuration.Constants;
import consensus.controllers.CacheManager;
import consensus.controllers.Channels;
import consensus.controllers.Election;
import consensus.models.Entry;
import consensus.models.VoteRequest;
import consensus.models.VoteResponse;
import controllers.Connection;
import models.Host;
import models.Packet;
import org.junit.jupiter.api.*;
import utils.JSONDeserializer;
import utils.PacketHandler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Tests the function in Election class
 *
 * @author Palak Jain
 */
public class ElectionTest {
    @BeforeAll
    public static void init() {
        Mock.mockDB();

        CacheManager.setLocal(new Host(0, "localhost", 1700, "Broker1"));
        CacheManager.addMember(new Host(0, "localhost", 1700, "Broker1"));
        CacheManager.addMember(new Host(1, "localhost", 1701, "Broker2"));
        CacheManager.addMember(new Host(2, "localhost", 1702, "Broker3"));

        CacheManager.initSentAndAckLength();
        Constants.ELECTION_MAX_TIME = Constants.FAULT_DETECTOR_MAX_VALUE = 2 * 3600000;
        Constants.ELECTION_MIN_TIME = Constants.FAULT_DETECTOR_MIN_VALUE = 3600000;

        CacheManager.setLocation("test/consensus/Broker1.log");
        CacheManager.setCommitLocation("test/commit/Broker1.log");
    }

    @BeforeEach
    public void beforeEach() {
        CacheManager.initSentAndAckLength();
        CacheManager.setCommitLength(0);

        for (int index = 0; index < 3; index++) {
            byte[] data = String.format("This is the sample test log line number %d.", index).getBytes(StandardCharsets.UTF_8);
            if (!CacheManager.addEntry(data, 0, "localhost:1705", 0)) {
                break;
            }

            Entry entry = CacheManager.getEntry(index);
            entry.setData(data);
        }
    }

    /**
     * Test whether broker become leader after receiving maximum votes from maximum number of followers
     */
    @Test
    public void processVoteResponse_receiveMaxVotes_shouldBecomeLeader() {
        //Setup
        CacheManager.setCurrentRole(Constants.ROLE.CANDIDATE.ordinal());
        VoteResponse voteResponse1 = new VoteResponse(0, 0 , true);
        VoteResponse voteResponse2 = new VoteResponse(1, 0, true);
        VoteResponse voteResponse3 = new VoteResponse(2, 0, true);

        //Execution
        Election.get().processVoteResponse(voteResponse1);
        Election.get().processVoteResponse(voteResponse2);
        Election.get().processVoteResponse(voteResponse3);

        //Verification
        Assertions.assertEquals(Constants.ROLE.LEADER.ordinal(), CacheManager.getCurrentRole());

        //Cleanup
        CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
    }

    /**
     * Test whether broker remain candidate after receiving less votes from followers
     */
    @Test
    public void processVoteResponse_receiveMinVotes_shouldRemainCandidate() {
        //Setup
        CacheManager.setCurrentRole(Constants.ROLE.CANDIDATE.ordinal());
        VoteResponse voteResponse1 = new VoteResponse(0, 0 , true);
        VoteResponse voteResponse2 = new VoteResponse(1, 0, false);
        VoteResponse voteResponse3 = new VoteResponse(2, 0, true);

        //Execution
        Election.get().processVoteResponse(voteResponse1);
        Election.get().processVoteResponse(voteResponse2);
        Election.get().processVoteResponse(voteResponse3);

        //Verification
        Assertions.assertEquals(Constants.ROLE.CANDIDATE.ordinal(), CacheManager.getCurrentRole());

        //Cleanup
        CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
    }

    /**
     * Tests whether broker become follower when found follower with higher term
     */
    @Test
    public void processVoteResponse_foundHigherTerm_shouldBecomeFollower() {
        //Setup
        CacheManager.setCurrentRole(Constants.ROLE.CANDIDATE.ordinal());
        VoteResponse voteResponse1 = new VoteResponse(0, 1 , true);
        VoteResponse voteResponse2 = new VoteResponse(1, 0, false);
        VoteResponse voteResponse3 = new VoteResponse(2, 0, true);

        //Execution
        Election.get().processVoteResponse(voteResponse1);
        Election.get().processVoteResponse(voteResponse2);
        Election.get().processVoteResponse(voteResponse3);

        //Verification
        Assertions.assertEquals(Constants.ROLE.FOLLOWER.ordinal(), CacheManager.getCurrentRole());

        //Cleanup
        CacheManager.setTerm(0);
        CacheManager.setCurrentRole(Constants.ROLE.FOLLOWER.ordinal());
    }

    /**
     * Test whether the follower reject the vote if already voted for other follower
     */
    @Test
    public void processVoteRequest_alreadyVoted_shouldRejectVote() {
        //Setup
        ByteArrayOutputStream expectedOutputStream = mockConnection(CacheManager.getNeighbor(1));
        CacheManager.setVoteFor(1, true);
        VoteRequest voteRequest = new VoteRequest(1, 0, 3, 0);

        //Execution
        Election.get().processVoteRequest(voteRequest);

        //Verification
        byte[] bytesReceived = Arrays.copyOfRange(expectedOutputStream.toByteArray(), 4, expectedOutputStream.toByteArray().length);
        byte[] data = PacketHandler.getData(bytesReceived);
        if (data != null) {
            Packet<VoteResponse> packet = JSONDeserializer.deserializePacket(data, new TypeToken<Packet<VoteResponse>>(){}.getType());

            if (packet != null && packet.getObject() != null) {
                Assertions.assertFalse(packet.getObject().isSuccess());
            } else {
                Assertions.fail();
            }
        } else {
            Assertions.fail();
        }

        //Cleanup
        CacheManager.setVoteFor(-1, true);
    }

    /**
     * Test whether the follower reject the vote if the candidate log is behind follower's log
     */
    @Test
    public void processVoteRequest_foundInCompleteCandidate_shouldRejectVote() {
        //Setup
        ByteArrayOutputStream expectedOutputStream = mockConnection(CacheManager.getNeighbor(1));
        CacheManager.setVoteFor(1, true);
        VoteRequest voteRequest = new VoteRequest(1, 0, 0, 0);

        //Execution
        Election.get().processVoteRequest(voteRequest);

        //Verification
        byte[] bytesReceived = Arrays.copyOfRange(expectedOutputStream.toByteArray(), 4, expectedOutputStream.toByteArray().length);
        byte[] data = PacketHandler.getData(bytesReceived);
        if (data != null) {
            Packet<VoteResponse> packet = JSONDeserializer.deserializePacket(data, new TypeToken<Packet<VoteResponse>>(){}.getType());

            if (packet != null && packet.getObject() != null) {
                Assertions.assertFalse(packet.getObject().isSuccess());
            } else {
                Assertions.fail();
            }
        } else {
            Assertions.fail();
        }

        //Cleanup
        CacheManager.setVoteFor(-1, true);
    }

    @AfterEach
    public void afterEach() {
        File file = new File(CacheManager.getLocation());
        file.delete();
        file = new File(CacheManager.getCommitLocation());
        file.delete();

        CacheManager.removeEntryFrom(0);
        CacheManager.initSentAndAckLength();
        Channels.remove(CacheManager.getNeighbor(1).toString());
    }

    @AfterAll
    public static void afterAll() {
        CacheManager.removeMembers();
        CacheManager.setLocation(null);
        CacheManager.setCommitLocation(null);
    }

    /**
     * Mock the connection object for the given host
     */
    private ByteArrayOutputStream mockConnection(Host host) {
        Connection connection = new Connection(null, host.getAddress(), host.getPort());
        Channels.add(host.toString(), connection);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            Field outputStream = Connection.class.getDeclaredField("outputStream");
            outputStream.setAccessible(true);
            outputStream.set(connection, dataOutputStream);
        } catch (NoSuchFieldException | IllegalAccessException exception) {
            exception.printStackTrace();
        }

        return byteArrayOutputStream;
    }
}
