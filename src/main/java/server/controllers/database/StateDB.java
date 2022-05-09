package server.controllers.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.models.NodeState;

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
                String query = "INSERT into state VALUES (0, ?, ?, ?, ?);";

                execute(nodeState, con, query);
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
            String query = "SELECT * FROM state where id = 0";
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                nodeState = new NodeState(resultSet.getInt("term"),
                                          resultSet.getInt("votedFor"),
                                          resultSet.getInt("commitLength"),
                                          resultSet.getInt("leader"));
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
            String query = "UPDATE state SET term = ?, votedFor = ?, commitLength = ?, leader = ? WHERE id = 0";

            execute(nodeState, connection, query);
        } catch (SQLException sqlException) {
            logger.error("Error while updating the state of the node", sqlException);
        }
    }

    /**
     * Setting the status values at the appropriate places in prepared statement
     */
    private static void execute(NodeState nodeState, Connection con, String query) throws SQLException {
        PreparedStatement statement = con.prepareStatement(query);
        statement.setInt(0, nodeState.getTerm());
        statement.setInt(1, nodeState.getVotedFor());
        statement.setInt(2, nodeState.getCommitLength());
        statement.setInt(3, nodeState.getCurrentLeader());

        statement.executeUpdate();
    }
}
