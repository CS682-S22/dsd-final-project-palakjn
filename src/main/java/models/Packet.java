package models;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import utils.Strings;

import java.nio.charset.StandardCharsets;

/**
 * Responsible for serializing the packet to String and getting the packet in byte
 *
 * @author Palak Jain
 */
public class Packet {

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
