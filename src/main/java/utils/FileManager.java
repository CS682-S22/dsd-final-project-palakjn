package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import consensus.controllers.CacheManager;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Responsible for all the I/O tasks.
 *
 * @author Palak Jain
 */
public class FileManager {
    private static final Logger logger = LogManager.getLogger(FileManager.class);
    private static String location;

    public static void init(String location) {
        FileManager.location = location;
    }

    /**
     * Write the given log data at the given position
     */
    public static boolean write(byte[] logData, int fromOffset) {
        boolean isSuccess = false;

        try (FileOutputStream outputStream = new FileOutputStream(location, true)) {
            outputStream.getChannel().position(fromOffset);
            outputStream.write(logData);
            outputStream.flush();

            logger.info(String.format("[%s] Wrote %d bytes to location %s at the position %d.", CacheManager.getLocal().toString(), logData.length, location, fromOffset));
            isSuccess = true;
        } catch (IndexOutOfBoundsException | IOException exception) {
            logger.error(String.format("Unable to open the file at the location %s.", location), exception);
        }

        return isSuccess;
    }

    /**
     * Reading the log from given offset of the given length
     */
    public static byte[] read(int fromOffset, int length) {
        byte[] data = new byte[length];

        try (FileInputStream inputStream = new FileInputStream(location)) {
            inputStream.getChannel().position(fromOffset);
            int result = inputStream.read(data);
            if(result != length) {
                logger.warn(String.format("[%s] Not able to send data. Read %d number of bytes. Expected %d number of bytes.", CacheManager.getLocal().toString(), result, length));
                data = null;
            }
        } catch (IndexOutOfBoundsException | IOException exception) {
            logger.error(String.format("Unable to open the file at the location %s.", location), exception);
        }

        return data;
    }
}
