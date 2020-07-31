/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

class RaftMonitorTest
{
    private final AssertableLogProvider user = new AssertableLogProvider();
    private final AssertableLogProvider debug = new AssertableLogProvider();

    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final MemberId myself = new MemberId( UUID.randomUUID() );

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
        raftBinderMonitor.boundToRaftThroughTopology( namedDatabaseId, raftId, myself );

        var expected = format( "Bound database '%s' to raft with id '%s' as member id '%s'.",
                namedDatabaseId.name(), raftId.uuid(), myself.getUuid() );
        assertThat( user ).containsMessages( expected );
        assertThat( debug ).containsMessages( expected );
    }

    @Test
    void shouldLogWhenRaftIdReadFromDisk()
    {
        var raftId = RaftId.from( namedDatabaseId.databaseId() );
        raftBinderMonitor.boundToRaftFromDisk( namedDatabaseId, raftId, myself );

        var expected = format( "Bound database '%s' to raft with id '%s' as member id '%s', found on disk.",
                namedDatabaseId.name(), raftId.uuid(), myself.getUuid() );
        assertThat( user ).containsMessages( expected );
        assertThat( debug ).containsMessages( expected );
    }

    @Test
    void shouldWriteToUserLogWhenWaitingForCoreMembers()
    {
        raftBinderMonitor.waitingForCoreMembers( namedDatabaseId, 42 );

        assertThat( user ).forClass( RaftMonitor.class ).forLevel( INFO )
                .containsMessageWithArguments( "Database '%s' is waiting for a total of %d core members" , namedDatabaseId.name(), 42 );
    }

    @Test
    void shouldWriteToUserLogWhenWaitingForBootstrap()
    {
        raftBinderMonitor.waitingForBootstrap( namedDatabaseId );

        assertThat( user ).forClass( RaftMonitor.class ).forLevel( INFO )
                .containsMessageWithArguments( "Database '%s' is waiting for bootstrap by other instance" , namedDatabaseId.name() );
    }

    @Test
    void shouldWriteToUserLogWhenStartingSnapshotDownload()
    {
        snapshotMonitor.startedDownloadingSnapshot( namedDatabaseId );

        assertThat( user ).forClass( RaftMonitor.class ).forLevel( INFO )
                .containsMessageWithArguments( "Started downloading snapshot for database '%s'", namedDatabaseId.name() );
    }

    @Test
    void shouldWriteToUserLogWhenFinishedSnapshotDownload()
    {
        snapshotMonitor.downloadSnapshotComplete( namedDatabaseId );

        assertThat( user ).forClass( RaftMonitor.class ).forLevel( INFO )
                .containsMessageWithArguments( "Download of snapshot for database '%s' complete" , namedDatabaseId.name() );
    }
}
