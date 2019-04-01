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

public interface CoreStateRepository
{
    void augmentSnapshot( String databaseName, CoreSnapshot coreSnapshot );

    void installSnapshotForDatabase( String databaseName, CoreSnapshot coreSnapshot );

    void installSnapshotForRaftGroup( CoreSnapshot coreSnapshot );

    void flush( long lastApplied ) throws IOException;

    CommandDispatcher commandDispatcher();

    long getLastAppliedIndex();

    long getLastFlushed();

    Map<String,DatabaseCoreStateComponents> getAllDatabaseStates();

    Optional<DatabaseCoreStateComponents> getDatabaseState( String databaseName );
}
