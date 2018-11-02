/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.outcome;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.logging.Log;

public interface RaftLogCommand
{
    interface Handler
    {
        void append( long baseIndex, RaftLogEntry... entries ) throws IOException;
        void truncate( long fromIndex ) throws IOException;
        void prune( long pruneIndex );
    }

    void dispatch( Handler handler ) throws IOException;

    void applyTo( RaftLog raftLog, Log log ) throws IOException;

    void applyTo( InFlightCache inFlightCache, Log log );
}
