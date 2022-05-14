package consensus.controllers.database;

import consensus.controllers.CacheManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import consensus.models.NodeState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class holds all the queries being made to the state table
 *
 * @author Palak Jain
 */
public class StateDB {
    private static final Logger logger = LogManager.getLogger(StateDB.class);

    /**
     * Insert new if not exist/Update existing row to the table setting the current status of the server
     */
    public static void upsert(NodeState nodeState) {
        NodeState state = get();

        if (state == null) {
            try (Connection con = DataSource.getConnection()) {
                String query = "INSERT into state VALUES (?, ?, ?, ?, ?, ?, ?);";

                PreparedStatement statement = con.prepareStatement(query);
                statement.setInt(1, CacheManager.getLocal().getId());
                statement.setInt(2, nodeState.getTerm());
                statement.setInt(3, nodeState.getVotedFor());
                statement.setInt(4, nodeState.getCommitLength());
                statement.setInt(5, nodeState.getCurrentLeader());
                statement.setString(6, nodeState.getCommitLocation());
                statement.setString(7, nodeState.getLocation());

                statement.executeUpdate();
            } catch (SQLException sqlException) {
                logger.error(String.format("Error while inserting event %s to the table. \n", nodeState.toString()), sqlException);
            }
        } else {
            update(nodeState);
        }
    }

    /**
     * Get the status of the server from DB
     */
    public static NodeState get() {
        NodeState nodeState = null;

        try (Connection connection = DataSource.getConnection()) {
            String query = "SELECT * FROM state where id = ?";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(1, CacheManager.getLocal().getId());
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                nodeState = new NodeState(resultSet.getInt("term"),
                                          resultSet.getInt("votedFor"),
                                          resultSet.getInt("commitLength"),
                                          resultSet.getInt("leader"),
                                          resultSet.getString("commitLocation"),
                                          resultSet.getString("location"));
            }
        } catch (SQLException sqlException) {
            logger.error("Error while getting the state of the node", sqlException);
        }

        return nodeState;
    }

    /**
     * Update the status of the server
     */
    public static void update(NodeState nodeState) {
        try (Connection connection = DataSource.getConnection()) {
            String query = "UPDATE state SET term = ?, votedFor = ?, commitLength = ?, leader = ?, commitLocation = ?, location = ? WHERE id = ?";

            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(1, nodeState.getTerm());
            statement.setInt(2, nodeState.getVotedFor());
            statement.setInt(3, nodeState.getCommitLength());
            statement.setInt(4, nodeState.getCurrentLeader());
            statement.setString(5, nodeState.getCommitLocation());
            statement.setString(6, nodeState.getLocation());
            statement.setInt(7, CacheManager.getLocal().getId());

            statement.executeUpdate();
        } catch (SQLException sqlException) {
            logger.error("Error while updating the state of the node", sqlException);
        }
    }
}
