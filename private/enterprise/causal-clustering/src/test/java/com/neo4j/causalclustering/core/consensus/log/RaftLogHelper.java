/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;

public class RaftLogHelper
{
    private RaftLogHelper()
    {
    }

    public static RaftLogEntry readLogEntry( ReadableRaftLog raftLog, long index ) throws IOException
    {
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( index ) )
        {
            if ( cursor.next() )
            {
                return cursor.get();
            }
        }

        //todo: do not do this and update RaftLogContractTest to not depend on this exception.
        throw new IOException( "Asked for raft log entry at index " + index + " but it was not found" );
    }

    public static Matcher<? super RaftLog> hasNoContent( long index )
    {
        return new TypeSafeMatcher<RaftLog>()
        {
            @Override
            protected boolean matchesSafely( RaftLog log )
            {
                try
                {
                    readLogEntry( log, index );
                }
                catch ( IOException e )
                {
                    // oh well...
                    return true;
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Log should not contain entry at index " ).appendValue( index );
            }
        };
    }
}
