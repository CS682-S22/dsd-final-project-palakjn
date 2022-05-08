package utils;

import application.Constants;
import models.Header;
import models.Host;
import models.Packet;

import java.nio.ByteBuffer;

/**
 * Responsible for generating packets to send via the channel from one host to another.
 *
 * @author Palak Jain
 */
public class PacketHandler {

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
}
