/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

public interface ReadableRaftLog
{
    /**
     * @return The index of the last appended entry.
     */
    long appendIndex();

    /**
     * @return The index immediately preceding entries in the log.
     */
    long prevIndex();

    /**
     * Reads the term associated with the entry at the supplied index.
     *
     * @param logIndex The index of the log entry.
     * @return The term of the entry, or -1 if the entry does not exist
     */
    long readEntryTerm( long logIndex ) throws IOException;

    /**
     * Returns a {@link RaftLogCursor} of {@link RaftLogEntry}s from the specified index until the end of the log
     * @param fromIndex The log index at which the cursor should be positioned
     */
    RaftLogCursor getEntryCursor( long fromIndex ) throws IOException;
}
