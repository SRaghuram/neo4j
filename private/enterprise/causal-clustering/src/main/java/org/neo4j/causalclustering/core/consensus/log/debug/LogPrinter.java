/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.debug;

import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;

public class LogPrinter
{
    private final ReadableRaftLog raftLog;

    public LogPrinter( ReadableRaftLog raftLog )
    {
        this.raftLog = raftLog;
    }

    public void print( PrintStream out ) throws IOException
    {
        out.println( String.format( "%1$8s %2$5s  %3$2s %4$s", "Index", "Term", "C?", "Content"));
        long index = 0L;
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( 0 ) )
        {
            while ( cursor.next() )
            {
                RaftLogEntry raftLogEntry = cursor.get();
                out.printf("%8d %5d %s", index, raftLogEntry.term(), raftLogEntry.content());
                index++;
            }
        }
    }
}
