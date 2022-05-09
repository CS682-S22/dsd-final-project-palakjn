package server.controllers.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.models.Entry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class holds all the queries being made to the Entries table.
 *
 * @author Palak Jain
 */
public class EntryDB {
    private static final Logger logger = LogManager.getLogger(EntryDB.class);

    /**
     * Insert new entry metadata to the table
     */
    public static void insert(Entry entry) {
        try (Connection connection = DataSource.getConnection()) {
            String query = "INSERT INTO entries VALUES (?, ?, ?, ?, ?)";

            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(0, entry.getTerm());
            statement.setInt(1, entry.getFromOffset());
            statement.setInt(2, entry.getToOffset());
            statement.setString(3, entry.getClientId());
            statement.setInt(4, entry.getReceivedOffset());

            statement.executeUpdate();
        } catch (SQLException sqlException) {
            logger.error(String.format("Error while inserting the entry %s to the table. ", entry.toString()) , sqlException);
        }
    }

    /**
     * Delete entry metadata from the given starting offset
     */
    public static void deleteFrom(int fromOffset) {
        try (Connection connection = DataSource.getConnection()) {
            String query = "DELETE FROM entries WHERE fromOffset >= ?";

            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(0, fromOffset);

            statement.executeUpdate();
        } catch (SQLException sqlException) {
            logger.error(String.format("Unable to delete entries from offset %d. ", fromOffset) , sqlException);
        }
    }

    /**
     * Get all the entries' metadata from the given starting offset
     */
    public static List<Entry> getFrom(int fromOffset) {
        List<Entry> entries = null;

        try (Connection connection = DataSource.getConnection()) {
            String query = "SELECT * from entries WHERE fromOffset >= ?";

            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(0, fromOffset);

            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Entry entry = new Entry(resultSet.getInt("term"),
                                    resultSet.getInt("fromOffset"),
                                    resultSet.getInt("toOffset"),
                                    resultSet.getString("clientId"),
                                    resultSet.getInt("clientOffset"));
                if (entries == null) { entries = new ArrayList<>(); }
                entries.add(entry);
            }
        } catch (SQLException sqlException) {
            logger.error(String.format("Error while getting entries from offset %s. ", fromOffset), sqlException);
        }

        return entries;
    }
}
