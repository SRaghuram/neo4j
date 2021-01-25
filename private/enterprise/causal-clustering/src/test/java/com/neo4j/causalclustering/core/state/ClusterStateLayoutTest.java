/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Set;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.CausalClusteringSettings.DEFAULT_CLUSTER_STATE_DIRECTORY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATA_DIR_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.set;

@TestDirectoryExtension
class ClusterStateLayoutTest
{
    private static final String DATABASE_NAME = "my_database";

    @Inject
    private TestDirectory testDirectory;

    private Path clusterStateDir;
    private ClusterStateLayout layout;

    @BeforeEach
    void setUp()
    {
        clusterStateDir = testDirectory.directory( DEFAULT_DATA_DIR_NAME ).resolve( DEFAULT_CLUSTER_STATE_DIRECTORY_NAME );
        layout = ClusterStateLayout.of( clusterStateDir );
    }

    @Test
    void shouldExposeVersionStateFile()
    {
        assertEquals( clusterStateDir.resolve( "version-state" ).resolve( "version" ), layout.clusterStateVersionFile() );
    }

    @Test
    void shouldExposeMemberIdStateFile()
    {
        assertEquals( clusterStateDir.resolve( "core-member-id-state" ).resolve( "core-member-id" ), layout.oldMemberIdStateFile() );
    }

    @Test
    void shouldExposeRaftMemberIdStateFile()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "raft-member-id-state" ).resolve( "raft-member-id" ),
                layout.raftMemberIdStateFile( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftIdStateFile()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "raft-id-state" ).resolve( "raft-id" ),
                layout.raftGroupIdFile( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeQuarantineMarkerStateFile()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "quarantine-marker-state" )
                        .resolve( "quarantine-marker" ), layout.quarantineMarkerStateFile( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeLeaseStateDirectory()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "lease-state" ),
                layout.leaseStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeLastFlushedStateDirectory()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "last-flushed-state" ),
                layout.lastFlushedStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftMembershipStateDirectory()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "membership-state" ),
                layout.raftMembershipStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftLogDirectory()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "raft-log" ),
                layout.raftLogDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeSessionTrackerDirectory()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "session-tracker-state" ),
                layout.sessionTrackerDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftTermStateDirectory()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "term-state" ),
                layout.raftTermStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftVoteStateDirectory()
    {
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "vote-state" ),
                layout.raftVoteStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldListGlobalAndDatabaseEntriesEntries()
    {
        Set<CoreStateFiles<?>> types = set(
                CoreStateFiles.RAFT_MEMBER_ID,
                CoreStateFiles.RAFT_GROUP_ID,
                CoreStateFiles.QUARANTINE_MARKER,
                CoreStateFiles.OLD_CORE_MEMBER_ID,
                CoreStateFiles.SESSION_TRACKER,
                CoreStateFiles.RAFT_TERM,
                CoreStateFiles.RAFT_LOG );

        Set<Path> expected = set(
                clusterStateDir.resolve( "core-member-id-state" ),
                clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "raft-member-id-state" ),
                clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "raft-id-state" ),
                clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "quarantine-marker-state" ),
                clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "session-tracker-state" ),
                clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "session-tracker-state" ),
                clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "term-state" ),
                clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "raft-log" )
        );

        Set<Path> actual = layout.listGlobalAndDatabaseDirectories( DATABASE_NAME, types::contains );

        assertEquals( expected, actual );
    }

    @Test
    void shouldWorkNonDefaultDirectory()
    {
        var clusterStateDir = testDirectory.directory( "different" );
        var layout = ClusterStateLayout.of( clusterStateDir );

        assertEquals( clusterStateDir.resolve( "version-state" ).resolve( "version" ), layout.clusterStateVersionFile() );
        assertEquals( clusterStateDir.resolve( "db" ).resolve( DATABASE_NAME ).resolve( "raft-id-state" ).resolve( "raft-id" ),
                layout.raftGroupIdFile( DATABASE_NAME ) );

        assertNotEquals( layout.clusterStateVersionFile(), this.layout.clusterStateVersionFile() );
        assertNotEquals( layout.raftGroupIdFile( DATABASE_NAME ), this.layout.raftGroupIdFile( DATABASE_NAME ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"./foo", "../foo", "file://foo", "http://foo", "/var/run", "C:\\Windows"} )
    void shouldNotAllowInvalidDatabaseNames( String maliciousName )
    {
        assertThrows( IllegalArgumentException.class, () -> layout.raftGroupDir( maliciousName ) );
        assertThrows( IllegalArgumentException.class, () -> layout.raftGroupIdFile( maliciousName ) );
    }
}
