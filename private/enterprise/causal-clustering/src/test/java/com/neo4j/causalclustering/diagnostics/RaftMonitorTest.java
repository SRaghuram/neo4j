/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class RaftMonitorTest
{
    private final AssertableLogProvider user = new AssertableLogProvider();
    private final AssertableLogProvider debug = new AssertableLogProvider();

    private final DatabaseId databaseId = new DatabaseId( "smurf" );

    private RaftBinder.Monitor raftBinderMonitor;
    private PersistentSnapshotDownloader.Monitor snapshotMonitor;

    @BeforeEach
    void setUp()
    {
        var monitors = new Monitors();
        RaftMonitor.register( new SimpleLogService( user, debug ), monitors );

        raftBinderMonitor = monitors.newMonitor( RaftBinder.Monitor.class );
        snapshotMonitor = monitors.newMonitor( PersistentSnapshotDownloader.Monitor.class );
    }

    @Test
    void shouldNotDuplicateToAnyLog()
    {
        var raftId = new RaftId( UUID.randomUUID() );
        raftBinderMonitor.boundToRaft( databaseId, raftId );

        var expected = equalToIgnoringCase( format( "Bound database '%s' to raft with id '%s'.", databaseId.name(), raftId.uuid() ) );
        user.assertContainsExactlyOneMessageMatching( expected );
        debug.assertContainsExactlyOneMessageMatching( expected );
    }

    @Test
    void shouldWriteToUserLogWhenWaitingForCoreMembers()
    {
        raftBinderMonitor.waitingForCoreMembers( databaseId, 42 );

        user.assertExactly( inLog( RaftMonitor.class ).info( containsString( "Database '%s' is waiting for a total of %d core members" ), "smurf", 42 ) );
    }

    @Test
    void shouldWriteToUserLogWhenWaitingForBootstrap()
    {
        raftBinderMonitor.waitingForBootstrap( databaseId );

        user.assertExactly( inLog( RaftMonitor.class ).info( containsString( "Database '%s' is waiting for bootstrap by other instance" ), "smurf" ) );
    }

    @Test
    void shouldWriteToUserLogWhenStartingSnapshotDownload()
    {
        snapshotMonitor.startedDownloadingSnapshot( databaseId );

        user.assertExactly( inLog( RaftMonitor.class ).info( containsString( "Started downloading snapshot for database '%s'" ), "smurf" ) );
    }

    @Test
    void shouldWriteToUserLogWhenFinishedSnapshotDownload()
    {
        snapshotMonitor.downloadSnapshotComplete( databaseId );

        user.assertExactly( inLog( RaftMonitor.class ).info( containsString( "Download of snapshot for database '%s' complete" ), "smurf" ) );
    }
}
