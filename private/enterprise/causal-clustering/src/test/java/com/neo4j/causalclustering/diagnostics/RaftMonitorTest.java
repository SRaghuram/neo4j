/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotMonitor;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    private final RaftMemberId myself = IdFactory.randomRaftMemberId();

    private RaftBinder.Monitor raftBinderMonitor;
    private CoreSnapshotMonitor snapshotMonitor;

    @BeforeEach
    void setUp()
    {
        var monitors = new Monitors();
        RaftMonitor.register( new SimpleLogService( user, debug ), monitors, Clocks.systemClock() );

        raftBinderMonitor = monitors.newMonitor( RaftBinder.Monitor.class );
        snapshotMonitor = monitors.newMonitor( CoreSnapshotMonitor.class );
    }

    @Test
    void shouldLogWhenRaftIdReadFromTopology()
    {
        raftBinderMonitor.boundToRaftThroughTopology( namedDatabaseId, myself );

        var expected = format( "Bound as %s to %s published in discovery", myself, namedDatabaseId );
        assertThat( debug ).containsMessages( expected );
    }

    @Test
    void shouldLogWhenRaftIdReadFromDisk()
    {
        raftBinderMonitor.boundToRaftFromDisk( namedDatabaseId, myself );

        var expected = format( "Bound as %s to %s existing on disk", myself, namedDatabaseId );
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
