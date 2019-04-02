/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.neo4j.kernel.database.DatabaseId;

public interface CoreStateRepository
{
    void augmentSnapshot( DatabaseId databaseId, CoreSnapshot coreSnapshot );

    void installSnapshotForDatabase( DatabaseId databaseId, CoreSnapshot coreSnapshot );

    void installSnapshotForRaftGroup( CoreSnapshot coreSnapshot );

    void flush( long lastApplied ) throws IOException;

    CommandDispatcher commandDispatcher();

    long getLastAppliedIndex();

    long getLastFlushed();

    Map<DatabaseId,DatabaseCoreStateComponents> getAllDatabaseStates();

    Optional<DatabaseCoreStateComponents> getDatabaseState( DatabaseId databaseId );
}
