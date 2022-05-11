package models;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;
import utils.Strings;

import java.nio.charset.StandardCharsets;

/**
 * Responsible for serializing the packet to String and getting the packet in byte
 *
 * @author Palak Jain
 */
public class Packet<T> {
    @Expose
    private int type;
    @Expose
    private int status;
    @Expose
    private T object;

    public Packet(int type, int status, T object) {
        this.type = type;
        this.status = status;
        this.object = object;
    }

    public Packet(int type, int status) {
        this.type = type;
        this.status = status;
    }

    /**
     * Get the type of the packet
     */
    public int getType() {
        return type;
    }

    /**
     * Set the type of the packet
     */
    public void setType(int type) {
        this.type = type;
    }

    /**
     * Get the status of the packet in response to the previous request being made
     */
    public int getStatus() {
        return status;
    }

    /**
     * Set the status of the packet in response to the previous request being made
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * Get the object passes as part of packet
     */
    public T getObject() {
        return object;
    }

    /**
     * Set the object passed as part of packet
     */
    public void setObject(T object) {
        this.object = object;
    }

    /**
     * Format the object to JSON and return it as a string
     */
    public String toString() {
        String stringFormat = null;

        try {
            Gson gson = new GsonBuilder()
                    .excludeFieldsWithoutExposeAnnotation()
                    .create();
            stringFormat = gson.toJson(this);
        } catch (JsonSyntaxException exception) {
            System.out.println("Unable to convert the object to json");
        }

        return stringFormat;
    }

    /**
     * Format the object to JSON and return it as a byte array
     */
    public byte[] toByte() {
        byte[] bytes = null;
        String json = toString();

        if (!Strings.isNullOrEmpty(json)) {
            bytes = json.getBytes(StandardCharsets.UTF_8);
        }

        return bytes;
    }
}
