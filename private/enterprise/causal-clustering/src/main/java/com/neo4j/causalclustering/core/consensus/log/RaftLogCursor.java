/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

import org.neo4j.cursor.RawCursor;

public interface RaftLogCursor extends RawCursor<RaftLogEntry,Exception>
{
    @Override
    boolean next() throws IOException;

    @Override
    void close() throws IOException;

    long index();

    static RaftLogCursor empty()
    {
        return new RaftLogCursor()
        {
            @Override
            public boolean next()
            {
                return false;
            }

            @Override
            public void close()
            {
            }

            @Override
            public long index()
            {
                return -1;
            }

            @Override
            public RaftLogEntry get()
            {
                throw new IllegalStateException();
            }
        };
    }
}
