package org.neo4j.storageengine;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.RecordState;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.storageengine.api.StorageCommand;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class MyTransactionState implements RecordState {

    MyStore myStore;
    MyTransactionState(MyStore store)
    {
        myStore = store;
    }
    @Override
    public boolean hasChanges() {
        return false;
    }

    @Override
    public void extractCommands(Collection<StorageCommand> target) throws TransactionFailureException {

        target.add(new StorageCommand.TokenCommand() {

            @Override
            public int tokenId() {
                return 0;
            }

            @Override
            public boolean isInternal() {
                return false;
            }

            @Override
            public void serialize(WritableChannel channel) throws IOException {

            }
        });
    }
}
