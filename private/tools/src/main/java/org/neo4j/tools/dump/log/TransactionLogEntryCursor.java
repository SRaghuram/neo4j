/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_COMMIT;

/**
 * Groups {@link LogEntry} instances transaction by transaction
 */
public class TransactionLogEntryCursor implements IOCursor<LogEntry[]>
{
    private final IOCursor<LogEntry> delegate;
    private final List<LogEntry> transaction = new ArrayList<>();

    public TransactionLogEntryCursor( IOCursor<LogEntry> delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public LogEntry[] get()
    {
        return transaction.toArray( new LogEntry[transaction.size()] );
    }

    @Override
    public boolean next() throws IOException
    {
        transaction.clear();
        LogEntry entry;
        while ( delegate.next() )
        {
            entry = delegate.get();
            transaction.add( entry );
            if ( isBreakPoint( entry ) )
            {
                return true;
            }
        }
        return !transaction.isEmpty();
    }

    private static boolean isBreakPoint( LogEntry entry )
    {
        byte type = entry.getType();
        return type == TX_COMMIT || type == CHECK_POINT;
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }
}
