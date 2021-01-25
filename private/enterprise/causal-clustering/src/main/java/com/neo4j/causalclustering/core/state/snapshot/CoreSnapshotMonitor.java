/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import org.neo4j.kernel.database.NamedDatabaseId;

public interface CoreSnapshotMonitor
{
    void startedDownloadingSnapshot( NamedDatabaseId namedDatabaseId );

    void downloadSnapshotComplete( NamedDatabaseId namedDatabaseId );
}
