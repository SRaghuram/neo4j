/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.explorer;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;

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
