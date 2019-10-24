/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Set;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.set;
import static org.neo4j.io.fs.FileUtils.path;

@TestDirectoryExtension
class ClusterStateLayoutTest
{
    private static final String DATABASE_NAME = "my_database";

    @Inject
    private TestDirectory testDirectory;

    private File dataDir;
    private ClusterStateLayout layout;

    @BeforeEach
    void setUp()
    {
        dataDir = testDirectory.directory( "data" );
        layout = ClusterStateLayout.of( dataDir );
    }

    @Test
    void shouldExposeVersionStateFile()
    {
        assertEquals( path( dataDir, "cluster-state", "version-state", "version" ), layout.clusterStateVersionFile() );
    }

    @Test
    void shouldExposeMemberIdStateFile()
    {
        assertEquals( path( dataDir, "cluster-state", "core-member-id-state", "core-member-id" ), layout.memberIdStateFile() );
    }

    @Test
    void shouldExposeRaftIdStateFile()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "raft-id-state", "raft-id" ), layout.raftIdStateFile( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeLeaseStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "lease-state" ), layout.leaseStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeLastFlushedStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "last-flushed-state" ), layout.lastFlushedStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftMembershipStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "membership-state" ), layout.raftMembershipStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftLogDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "raft-log" ), layout.raftLogDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeSessionTrackerDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "session-tracker-state" ), layout.sessionTrackerDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftTermStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "term-state" ), layout.raftTermStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldExposeRaftVoteStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_NAME, "vote-state" ), layout.raftVoteStateDirectory( DATABASE_NAME ) );
    }

    @Test
    void shouldListGlobalAndDatabaseEntriesEntries()
    {
        Set<CoreStateFiles<?>> types = set(
                CoreStateFiles.RAFT_ID,
                CoreStateFiles.CORE_MEMBER_ID,
                CoreStateFiles.SESSION_TRACKER,
                CoreStateFiles.RAFT_TERM,
                CoreStateFiles.RAFT_LOG );

        Set<File> expected = set(
                path( dataDir, "cluster-state", "core-member-id-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_NAME,"raft-id-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_NAME, "session-tracker-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_NAME, "session-tracker-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_NAME, "term-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_NAME, "raft-log" )
        );

        Set<File> actual = layout.listGlobalAndDatabaseDirectories( DATABASE_NAME, types::contains );

        assertEquals( expected, actual );
    }
}
