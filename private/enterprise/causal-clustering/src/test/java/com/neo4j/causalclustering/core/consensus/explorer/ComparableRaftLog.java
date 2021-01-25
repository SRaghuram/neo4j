/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;

import java.io.IOException;

public class ComparableRaftLog extends InMemoryRaftLog
{
    public ComparableRaftLog( ReadableRaftLog raftLog ) throws IOException
    {
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( 0 ) )
        {
            while ( cursor.next() )
            {
                append( cursor.get() );
            }
        }
    }
}
