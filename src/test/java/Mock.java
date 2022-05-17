import consensus.controllers.database.EntryDB;
import consensus.controllers.database.StateDB;
import consensus.models.Entry;
import consensus.models.NodeState;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Mock functions for testing purpose
 *
 * @author Palak Jain
 */
public class Mock {
    private volatile static boolean isSetup = false;

    private Mock() {}

    /**
     * Mock DB functions to do nothing
     */
    public synchronized static void mockDB() {
        if (!isSetup) {
            MockedStatic<StateDB> stateDB = Mockito.mockStatic(StateDB.class);
            stateDB.when(() -> StateDB.update(Mockito.any(NodeState.class))).then(invocationOnMock -> null);
            stateDB.when(() -> StateDB.upsert(Mockito.any(NodeState.class))).then(invocationOnMock -> null);
            stateDB.when(StateDB::get).thenReturn(new NodeState());

            MockedStatic<EntryDB> entryDB = Mockito.mockStatic(EntryDB.class);
            entryDB.when(() -> EntryDB.setCommitted(Mockito.anyInt(), Mockito.anyBoolean())).then(invocationOnMock -> null);
            entryDB.when(() -> EntryDB.insert(Mockito.any(Entry.class))).then(invocationOnMock -> null);
            entryDB.when(() -> EntryDB.deleteFrom(Mockito.anyInt())).then(invocationOnMock -> null);
            isSetup = true;
        }
    }
}
