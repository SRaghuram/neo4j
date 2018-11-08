/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

public interface CoreStateRepository
{
    void augmentSnapshot( CoreSnapshot coreSnapshot );

    void installSnapshot( CoreSnapshot coreSnapshot );

    void flush( long lastApplied ) throws IOException;

    CommandDispatcher commandDispatcher();

    long getLastAppliedIndex();

    long getLastFlushed();

    Map<String,PerDatabaseCoreStateComponents> getAllDatabaseStates();

    Optional<PerDatabaseCoreStateComponents> getDatabaseState( String databaseName );
}
