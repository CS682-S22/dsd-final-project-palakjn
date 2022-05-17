import com.google.gson.reflect.TypeToken;
import consensus.controllers.CacheManager;
import consensus.controllers.Channels;
import consensus.controllers.Replication;
import consensus.models.AppendEntriesRequest;
import consensus.models.AppendEntriesResponse;
import consensus.models.Entry;
import controllers.Connection;
import models.Host;
import models.Packet;
import org.junit.jupiter.api.*;
import utils.FileManager;
import utils.JSONDeserializer;
import utils.PacketHandler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Responsible for testing functions in Replication class
 *
 * @author Palak Jain
 */
public class ReplicationTest {
    @BeforeAll
    public static void init() {
        Mock.mockDB();
        CacheManager.setLocal(new Host(0, "localhost", 1700, "Broker1"));
        CacheManager.setLocation("test/consensus/Broker1.log");
        CacheManager.setCommitLocation("test/commit/Broker1.log");

        CacheManager.addMember(new Host(1, "localhost", 1701, "Broker2"));
        CacheManager.addMember(new Host(2, "localhost", 1702, "Broker3"));

        CacheManager.initSentAndAckLength();
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
     * Test whether leader commit log for an entry if received maximum number of acknowledgement from the followers
     */
    @Test
    public void commitLogEntries_getMaxAck_shouldCommitToFile() {
        //Setup
        CacheManager.setAckedLength(0, 1);
        CacheManager.setAckedLength(1, 1);

        //Execution
        Replication.get().commitLogEntries();

        //Verification
        Entry entry = CacheManager.getEntry(CacheManager.getCommitLength() - 1);
        if (entry != null) {
            byte[] data = FileManager.read(CacheManager.getCommitLocation(), entry.getFromOffset(), (entry.getToOffset() + 1) - entry.getFromOffset());
            if (data != null) {
                Assertions.assertEquals("This is the sample test log line number 0.", new String(data));
            } else {
                Assertions.fail();
            }
        } else {
            Assertions.fail();
        }
    }

    /**
     * Test whether leader set the entry as committed in MD when received maximum number of acknowledgement from the followers
     */
    @Test
    public void commitLogEntries_getMaxAck_shouldSetAsCommitted() {
        //Setup
        CacheManager.setAckedLength(0, 1);
        CacheManager.setAckedLength(1, 1);

        //Execution
        Replication.get().commitLogEntries();

        //Verification
        Entry entry = CacheManager.getEntry(CacheManager.getCommitLength() - 1);
        if (entry != null) {
            Assertions.assertTrue(entry.isCommitted());
        } else {
            Assertions.fail();
        }
    }

    /**
     * Test whether leader should not set the entry as committed when received less number of acknowledgements from the followers
     */
    @Test
    public void commitLogEntries_getLessAck_shouldNotSetAsCommitted() {
        //Setup
        CacheManager.setAckedLength(0, 1);
        CacheManager.setAckedLength(1, 0);
        CacheManager.setAckedLength(2, 0);

        //Execution
        Replication.get().commitLogEntries();

        //Verification
        Entry entry = CacheManager.getEntry(0);
        if (entry != null) {
            Assertions.assertFalse(entry.isCommitted());
        } else {
            Assertions.fail();
        }
    }

    /**
     * Test whether broker send three logs to the broker or not
     */
    @Test
    public void replicate_shouldSentNLogs() {
        //Setup
        CacheManager.setSentLength(1, 0);
        Host follower = CacheManager.getNeighbor(1);
        ByteArrayOutputStream expectedOutputStream = mockConnection(follower);

        //Execution
        Replication.get().replicate(0, follower);

        //Verification
        byte[] bytesReceived = Arrays.copyOfRange(expectedOutputStream.toByteArray(), 4, expectedOutputStream.toByteArray().length);
        byte[] data = PacketHandler.getData(bytesReceived);

        if (data != null) {
            Packet<AppendEntriesRequest> packet = JSONDeserializer.deserializePacket(data, new TypeToken<Packet<AppendEntriesRequest>>(){}.getType());
            if (packet != null && packet.getObject() != null) {
                AppendEntriesRequest appendEntriesRequest = packet.getObject();

                Assertions.assertEquals(3, appendEntriesRequest.getLength());
            } else {
                Assertions.fail();
            }
        } else {
            Assertions.fail();
        }
    }

    /**
     * Test whether broker send AppendEntries Packet even when there are no logs to send
     */
    @Test
    public void replicate_shouldSentEmptyPacket() {
        //Setup
        CacheManager.setSentLength(1, 3);
        Host follower = CacheManager.getNeighbor(1);
        ByteArrayOutputStream expectedOutputStream = mockConnection(follower);

        //Execution
        Replication.get().replicate(0, follower);

        //Verification
        byte[] bytesReceived = Arrays.copyOfRange(expectedOutputStream.toByteArray(), 4, expectedOutputStream.toByteArray().length);
        byte[] data = PacketHandler.getData(bytesReceived);

        if (data != null) {
            Packet<AppendEntriesRequest> packet = JSONDeserializer.deserializePacket(data, new TypeToken<Packet<AppendEntriesRequest>>(){}.getType());
            if (packet != null && packet.getObject() != null) {
                AppendEntriesRequest appendEntriesRequest = packet.getObject();

                Assertions.assertEquals(0, appendEntriesRequest.getLength());
            } else {
                Assertions.fail();
            }
        } else {
            Assertions.fail();
        }
    }

    /**
     * Test whether broker ignore duplicate logs
     */
    @Test
    public void writeLogs_receiveDuplicate_ShouldNotAddEntry() {
        //Setup
        List<Entry> suffix = new ArrayList<>();
        suffix.add(CacheManager.getEntry(1));
        suffix.add(CacheManager.getEntry(2));
        byte[] data = "This is the sample test log line number 3.".getBytes(StandardCharsets.UTF_8);
        Entry entry = new Entry(0, suffix.get(1).getToOffset() + 1, suffix.get(1).getToOffset() + 1 + data.length - 1, null,  suffix.get(1).getToOffset() + 1, false);
        entry.setData(data);
        suffix.add(entry);
        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(2,0, 1, 0, 0, suffix);

        //Execution
        Replication.get().writeLogs(appendEntriesRequest);

        //Verification
        Assertions.assertEquals(4, CacheManager.getLogLength());

        //Cleanup
        CacheManager.removeEntryFrom(3);
    }

    /**
     * Test whether broker ignore writing duplicate log to file
     */
    @Test
    public void writeLogs_receiveDuplicate_ShouldNotWriteToFile() {
        //Setup
        List<Entry> suffix = new ArrayList<>();
        suffix.add(CacheManager.getEntry(1));
        suffix.add(CacheManager.getEntry(2));
        byte[] data = "This is the sample test log line number 3.".getBytes(StandardCharsets.UTF_8);
        Entry entry = new Entry(0, suffix.get(1).getToOffset() + 1, suffix.get(1).getToOffset() + 1 + data.length - 1, null,  suffix.get(1).getToOffset() + 1, false);
        entry.setData(data);
        suffix.add(entry);
        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(2,0, 1, 0, 0, suffix);

        //Execution
        Replication.get().writeLogs(appendEntriesRequest);

        //Verification
        Assertions.assertEquals(entry.getToOffset() + 1, FileManager.getFileLength(CacheManager.getLocation()));

        //Cleanup
        CacheManager.removeEntryFrom(3);
    }

    /**
     * Test whether broker repair logs if found mismatch entries from the leader
     */
    @Test
    public void writeLogs_mismatchEntries_shouldRepairLogs() {
        //Setup
        //Adding new entries to current broker
        Entry prevEntry = CacheManager.getEntry(2);
        Entry entry1 = new Entry(0, prevEntry.getToOffset() + 1, prevEntry.getToOffset() + 1 + 5);
        Entry entry2 = new Entry(0, entry1.getToOffset() + 1, entry1.getToOffset() + 1 + 5);
        CacheManager.addEntry(entry1);
        CacheManager.addEntry(entry2);

        //Creating two suffix which leader is sending
        Entry entry3 = new Entry(1, prevEntry.getToOffset() + 1, prevEntry.getToOffset() + 1 + 5);
        entry3.setData("Hello1".getBytes(StandardCharsets.UTF_8));
        Entry entry4 = new Entry(1, entry3.getToOffset() + 1, entry3.getToOffset() + 1 + 5);
        entry4.setData("Hello2".getBytes(StandardCharsets.UTF_8));
        List<Entry> suffix = new ArrayList<>();
        suffix.add(entry3);
        suffix.add(entry4);

        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(2,0, 3, 0, 0, suffix);

        //Execution
        Replication.get().writeLogs(appendEntriesRequest);

        //Verification
        Assertions.assertEquals(5, CacheManager.getLogLength());

        CacheManager.removeEntryFrom(3);
    }

    /**
     * Test whether follower should commit logs when found that leader committed the log
     */
    @Test
    public void writeLogs_shouldCommitLogs() {
        //Setup
        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(2, 0, 3, 0, 3, new ArrayList<>());
        CacheManager.setCommitLength(0);
        CacheManager.setCommitLocation("test/commit/Broker2.log");

        //Execution
        Replication.get().writeLogs(appendEntriesRequest);

        //Verification
        boolean isCommitted = false;
        List<Entry> entries = CacheManager.getEntries(0, 3,true);
        int index = 0;
        for (Entry entry : entries) {
            byte[] data = FileManager.read(CacheManager.getCommitLocation(), entry.getFromOffset(), (entry.getToOffset() + 1) - entry.getFromOffset());
            if (data != null) {
                isCommitted = new String(data).equals(String.format("This is the sample test log line number %d.", index));
                index++;
            }

            if (!isCommitted) {
                break;
            }
        }

        Assertions.assertTrue(isCommitted);

        //Cleanup
        File file = new File(CacheManager.getCommitLocation());
        file.delete();
        CacheManager.setCommitLocation("test/commit/Broker1.log");
    }

    /**
     * Test whether follower should reject the appendEntries request if receives packet from leader whose term is less than follower's term
     */
    @Test
    public void appendEntries_logFromLessTerm_shouldReject() {
        //Setup
        CacheManager.setTerm(1);
        ByteArrayOutputStream expectedOutputStream = mockConnection(CacheManager.getNeighbor(1));
        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(1, 0, 3, 0,3, new ArrayList<>());

        //Execution
        Replication.get().appendEntries(appendEntriesRequest);

        //Verification
        byte[] bytesReceived = Arrays.copyOfRange(expectedOutputStream.toByteArray(), 4, expectedOutputStream.toByteArray().length);
        byte[] data = PacketHandler.getData(bytesReceived);
        if (data != null) {
            Packet<AppendEntriesResponse> packet = JSONDeserializer.deserializePacket(data, new TypeToken<Packet<AppendEntriesResponse>>(){}.getType());

            if (packet != null && packet.getObject() != null) {
                Assertions.assertFalse(packet.getObject().isSuccess());
            } else {
                Assertions.fail();
            }
        } else {
            Assertions.fail();
        }

        //Cleanup
        CacheManager.setTerm(0);
    }

    /**
     * Tests whether follower should reject the appendEntries request if received packet with prefixLen > Follower's log length
     */
    @Test
    public void appendEntries_UnMatchesLogLength_shouldReject() {
        //Setup
        ByteArrayOutputStream expectedOutputStream = mockConnection(CacheManager.getNeighbor(1));
        AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(1, 0, 3, 1,3, new ArrayList<>());

        //Execution
        Replication.get().appendEntries(appendEntriesRequest);

        //Verification
        byte[] bytesReceived = Arrays.copyOfRange(expectedOutputStream.toByteArray(), 4, expectedOutputStream.toByteArray().length);
        byte[] data = PacketHandler.getData(bytesReceived);
        if (data != null) {
            Packet<AppendEntriesResponse> packet = JSONDeserializer.deserializePacket(data, new TypeToken<Packet<AppendEntriesResponse>>(){}.getType());

            if (packet != null && packet.getObject() != null) {
                Assertions.assertFalse(packet.getObject().isSuccess());
            } else {
                Assertions.fail();
            }
        } else {
            Assertions.fail();
        }
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
