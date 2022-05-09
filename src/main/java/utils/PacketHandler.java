package utils;

import application.Constants;
import com.google.protobuf.InvalidProtocolBufferException;
import models.Header;
import models.Host;
import models.Packet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Responsible for generating packets to send via the channel from one host to another.
 *
 * @author Palak Jain
 */
public class PacketHandler {
    private static final Logger logger = LogManager.getLogger(PacketHandler.class);

    /**
     * Create the header part of the packet
     */
    public static byte[] createHeader(Constants.REQUESTER requester, Constants.HEADER_TYPE type, int seqNum, Host source, Host destination) {
        Header.Host sourceHost = Header.Host.newBuilder().setAddress(source.getAddress()).setPort(source.getPort()).build();
        Header.Host destinationHost = Header.Host.newBuilder().setAddress(destination.getAddress()).setPort(destination.getPort()).build();

        return Header.Content.newBuilder().setRequester(requester.ordinal()).setType(type.ordinal()).setSeqNum(seqNum).setSource(sourceHost).setDestination(destinationHost).build().toByteArray();
    }

    /**
     * Creates an acknowledgement packet for the file chunk with the given sequence number
     */
    public static byte[] createACK(Constants.REQUESTER requester, int seqNum, Host source, Host destination) {
        byte[] header = createHeader(requester, Constants.HEADER_TYPE.ACK, seqNum, source, destination);

        return ByteBuffer.allocate(4 + header.length).putInt(header.length).put(header).array();
    }

    /**
     * Creates negative acknowledgement packet for the file chunk with the given sequence number
     */
    public static byte[] createNACK(Constants.REQUESTER requester, int seqNum, Host source, Host destination) {
        byte[] header = createHeader(requester, Constants.HEADER_TYPE.NACK, seqNum, source, destination);

        return ByteBuffer.allocate(4 + header.length).putInt(header.length).put(header).array();
    }

    /**
     * Create the entire packet: header + body
     * @return byte array
     */
    public static byte[] createPacket(Constants.REQUESTER requester, Constants.HEADER_TYPE type, byte[] body, int seqNum, Host source, Host destination) {
        byte[] packetBytes = null;

        if (body != null) {
            byte[] header = createHeader(requester, type, seqNum, source, destination);
            packetBytes = ByteBuffer.allocate(4 + header.length + body.length).putInt(header.length).put(header).put(body).array();
        }

        return packetBytes;
    }

    /**
     * Create the entire packet: header + body
     * @return byte array
     */
    public static byte[] createPacket(Constants.REQUESTER requester, Constants.HEADER_TYPE type, Packet packet, int seqNum, Host source, Host destination) {
        byte[] body = packet.toByte();

        return createPacket(requester, type, body, seqNum, source, destination);
    }

    /**
     * Get the header part from the message
     * @param message the received message from the host
     * @return Decoded Header
     */
    public static Header.Content getHeader(byte[] message) {
        Header.Content header = null;

        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(message);
            //Reading length of the header
            int length = byteBuffer.getInt();
            //Reading main header part
            byte[] headerBytes = new byte[length];
            byteBuffer.get(headerBytes, 0, length);

            header =  Header.Content.parseFrom(headerBytes);
        } catch (InvalidProtocolBufferException | BufferUnderflowException exception) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);

            logger.error(String.format("Unable to read the header part from the received message. Error: %s.", pw.toString()));
        }

        return header;
    }

    /**
     * Get the actual file chunk data from the message
     * @param message the received message from the host
     * @return file data
     */
    public static byte[] getData(byte[] message) {
        byte[] content = null;

        try {
            content = Arrays.copyOfRange(message, getOffset(message), message.length);
        } catch (IndexOutOfBoundsException exception ) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);

            logger.error(String.format("Unable to read the data from the received message. Error: %s.", pw.toString()));
        }

        return content;
    }

    /**
     * Get the offset from where to read the body part of the message.
     * @param message the received message from the host
     * @return offset from where to read the message
     */
    public static int getOffset(byte[] message) {
        int position = 0;

        try {
            //4 + header length will give the position of the array where the next part of the message resides.
            position = 4 + ByteBuffer.wrap(message).getInt();
        } catch (BufferUnderflowException exception) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);

            logger.error(String.format("Unable to get the position to read next bytes after header from the received message. Error: %s.", pw.toString()));
        }

        return position;
    }
}
