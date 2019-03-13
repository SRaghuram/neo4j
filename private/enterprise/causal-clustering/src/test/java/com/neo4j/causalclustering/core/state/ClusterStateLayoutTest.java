/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Set;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.DATABASE;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.GLOBAL;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( TestDirectoryExtension.class )
class ClusterStateLayoutTest
{
    private static final String DATABASE_NAME = "my_database";

    @Inject
    private TestDirectory testDirectory;

    private File dataDirectory;
    private ClusterStateLayout layout;

    @BeforeEach
    void setUp()
    {
        dataDirectory = testDirectory.directory( "data" );
        layout = ClusterStateLayout.of( dataDirectory );
    }

    @Test
    void shouldExposeClusterIdStateFile()
    {
        assertEquals( globalStateFile( CoreStateFiles.CLUSTER_ID ), layout.clusterIdStateFile() );
    }

    @Test
    void shouldExposeMemberIdStateFile()
    {
        assertEquals( globalStateFile( CoreStateFiles.CORE_MEMBER_ID ), layout.memberIdStateFile() );
    }

    @Test
    void shouldExposeMultiClusteringDbNameStateFile()
    {
        assertEquals( globalStateFile( CoreStateFiles.DB_NAME ), layout.multiClusteringDbNameStateFile() );
    }

    @Test
    void shouldExposeIdAllocationStateDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.ID_ALLOCATION ), layout.idAllocationStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeLockTokenStateDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.LOCK_TOKEN ), layout.lockTokenStateDirectory( DATABASE_NAME ) );

    }

    @Test
    void shouldExposeLastFlushedStateDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.LAST_FLUSHED ), layout.lastFlushedStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftMembershipStateDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.RAFT_MEMBERSHIP ), layout.raftMembershipStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftLogDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.RAFT_LOG ), layout.raftLogDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeSessionTrackerDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.SESSION_TRACKER ), layout.sessionTrackerDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftTermStateDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.RAFT_TERM ), layout.raftTermStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftVoteStateDirectory()
    {
        assertEquals( databaseStateDirectory( CoreStateFiles.RAFT_VOTE ), layout.raftVoteStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeAllGlobalStateEntries()
    {
        Set<File> expected = CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == GLOBAL )
                .map( this::globalStateFile )
                .collect( toSet() );

        assertEquals( expected, layout.allGlobalStateEntries() );
    }

    @Test
    void shouldExposeAllDatabaseStateEntries()
    {
        Set<File> expected = CoreStateFiles.values()
                .stream()
                .filter( type -> type.scope() == DATABASE )
                .map( this::databaseStateDirectory )
                .collect( toSet() );

        assertEquals( expected, layout.allDatabaseStateEntries( DATABASE_NAME ) );
    }

    private File globalStateFile( CoreStateFiles<?> type )
    {
        return FileUtils.path( dataDirectory, "cluster-state", type.name() + "-state", type.name() );
    }

    private File databaseStateDirectory( CoreStateFiles<?> type )
    {
        return FileUtils.path( dataDirectory, "cluster-state", "db", DATABASE_NAME, type.name() + "-state" );
    }
}
