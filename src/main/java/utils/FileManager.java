package utils;

import consensus.controllers.CacheManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Responsible for all the I/O tasks.
 *
 * @author Palak Jain
 */
public class FileManager {
    private static final Logger logger = LogManager.getLogger(FileManager.class);

    /**
     * Write the given log data at the given position
     */
    public static boolean write(String location, byte[] logData, int fromOffset) {
        boolean isSuccess = false;

        try (RandomAccessFile writer = new RandomAccessFile(location, "rw"); FileChannel channel = writer.getChannel()) {
            channel.position(fromOffset);
            ByteBuffer buff = ByteBuffer.wrap(logData);
            channel.write(buff);

            logger.info(String.format("[%s] Wrote %d bytes to location %s at the position %d.", CacheManager.getLocal().toString(), logData.length, location, fromOffset));
            isSuccess = true;
        } catch (IndexOutOfBoundsException | IOException exception) {
            logger.error(String.format("Unable to open the file at the location %s.", location), exception);
        }

        return isSuccess;
    }

    /**
     * Append the given log to the end as a new line in String format
     */
    public static boolean write(String location, byte[] logData) {
        boolean isSuccess = false;

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(location, true))) {
            bufferedWriter.write(new String(logData));
            bufferedWriter.newLine();

            logger.info(String.format("[%s] Wrote %d bytes to location %s.", CacheManager.getLocal().toString(), logData.length, location));
            isSuccess = true;
        } catch (IndexOutOfBoundsException | IOException exception) {
            logger.error(String.format("Unable to open the file at the location %s.", location), exception);
        }

        return isSuccess;
    }

    /**
     * Reading the log from given offset of the given length
     */
    public static byte[] read(String location, int fromOffset, int length) {
        byte[] data = new byte[length];

        try (FileInputStream inputStream = new FileInputStream(location)) {
            inputStream.getChannel().position(fromOffset);
            int result = inputStream.read(data);
            if(result != length) {
                logger.warn(String.format("[%s] Read %d number of bytes. Expected %d number of bytes.", CacheManager.getLocal().toString(), result, length));
                data = null;
            }
        } catch (IndexOutOfBoundsException | IOException exception) {
            logger.error(String.format("Unable to open the file at the location %s.", location), exception);
        }

        return data;
    }

    /**
     * Get the length of the file
     */
    public static long getFileLength(String location) {
        long size = 0;

        File file = new File(location);
        size = file.length();

        return size;
    }
}
