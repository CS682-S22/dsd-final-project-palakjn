package server.controllers.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.models.SyncState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class holds all the queries being made to the ClientSync table
 *
 * @author Palak Jain
 */
public class ClientSyncDB {
    private static final Logger logger = LogManager.getLogger(StateDB.class);

    /**
     * Insert new if not exist/Update existing row to the table setting the data count received from the client
     */
    public static void upsert(SyncState syncState) {
        SyncState state = get(syncState.getClientId());

        if (state == null) {
            try (Connection con = DataSource.getConnection()) {
                String query = "INSERT into clientSync VALUES (?, ?);";

                PreparedStatement statement = con.prepareStatement(query);
                statement.setString(0, syncState.getClientId());
                statement.setInt(1, syncState.getReceivedCount());

                statement.executeUpdate();
            } catch (SQLException sqlException) {
                logger.error(String.format("Error while inserting event %s to the table. \n", syncState.toString()), sqlException);
            }
        } else {
            update(syncState);
        }
    }

    /**
     * Get the number of data received from the given client
     */
    public static SyncState get(String clientId) {
        SyncState syncState = null;

        try (Connection connection = DataSource.getConnection()) {
            String query = "SELECT * FROM clientSync where id = ?";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setString(0, clientId);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                syncState = new SyncState(resultSet.getString("id"),
                        resultSet.getInt("receivedCount"));
            }
        } catch (SQLException sqlException) {
            logger.error("Error while getting the state of the node", sqlException);
        }

        return syncState;
    }

    /**
     * Get list of the number of data received from all the clients
     */
    public static List<SyncState> get() {
        List<SyncState> syncStates = null;

        try (Connection connection = DataSource.getConnection()) {
            String query = "SELECT * FROM clientSync";
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                SyncState syncState = new SyncState(resultSet.getString("id"),
                        resultSet.getInt("receivedCount"));
                if (syncStates == null) { syncStates = new ArrayList<>(); }
                syncStates.add(syncState);
            }
        } catch (SQLException sqlException) {
            logger.error("Error while getting the state of the node", sqlException);
        }

        return syncStates;
    }

    /**
     * Update the data count received by the server from the client
     */
    public static void update(SyncState syncState) {
        try (Connection connection = DataSource.getConnection()) {
            String query = "UPDATE clientSync SET receivedCount = ? WHERE id = ?";

            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(0, syncState.getReceivedCount());
            statement.setString(1, syncState.getClientId());

            statement.executeUpdate();
        } catch (SQLException sqlException) {
            logger.error("Error while updating the state of the node", sqlException);
        }
    }
}
