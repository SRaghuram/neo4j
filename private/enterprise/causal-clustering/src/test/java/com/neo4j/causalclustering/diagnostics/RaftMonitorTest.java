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

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class RaftMonitorTest
{
    private final AssertableLogProvider user = new AssertableLogProvider();
    private final AssertableLogProvider debug = new AssertableLogProvider();

    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();

    private RaftBinder.Monitor raftBinderMonitor;
    private PersistentSnapshotDownloader.Monitor snapshotMonitor;

    @BeforeEach
    void setUp()
    {
        var monitors = new Monitors();
        RaftMonitor.register( new SimpleLogService( user, debug ), monitors, Clocks.systemClock() );

        raftBinderMonitor = monitors.newMonitor( RaftBinder.Monitor.class );
        snapshotMonitor = monitors.newMonitor( PersistentSnapshotDownloader.Monitor.class );
    }

    @Test
    void shouldNotDuplicateToAnyLog()
    {
        var raftId = RaftId.from( namedDatabaseId.databaseId() );
        raftBinderMonitor.boundToRaft( namedDatabaseId, raftId );

        var expected = equalToIgnoringCase( format( "Bound database '%s' to raft with id '%s'.", namedDatabaseId.name(), raftId.uuid() ) );
        user.rawMessageMatcher().assertContainsSingle( expected );
        debug.rawMessageMatcher().assertContainsSingle( expected );
    }

    @Test
    void shouldWriteToUserLogWhenWaitingForCoreMembers()
    {
        raftBinderMonitor.waitingForCoreMembers( namedDatabaseId, 42 );

        user.assertExactly( inLog( RaftMonitor.class ).info(
                containsString( "Database '%s' is waiting for a total of %d core members" ), namedDatabaseId.name(), 42 ) );
    }

    @Test
    void shouldWriteToUserLogWhenWaitingForBootstrap()
    {
        raftBinderMonitor.waitingForBootstrap( namedDatabaseId );

        user.assertExactly( inLog( RaftMonitor.class ).info(
                containsString( "Database '%s' is waiting for bootstrap by other instance" ), namedDatabaseId.name() ) );
    }

    @Test
    void shouldWriteToUserLogWhenStartingSnapshotDownload()
    {
        snapshotMonitor.startedDownloadingSnapshot( namedDatabaseId );

        user.assertExactly( inLog( RaftMonitor.class ).info(
                containsString( "Started downloading snapshot for database '%s'" ), namedDatabaseId.name() ) );
    }

    @Test
    void shouldWriteToUserLogWhenFinishedSnapshotDownload()
    {
        snapshotMonitor.downloadSnapshotComplete( namedDatabaseId );

        user.assertExactly( inLog( RaftMonitor.class ).info(
                containsString( "Download of snapshot for database '%s' complete" ), namedDatabaseId.name() ) );
    }
}
