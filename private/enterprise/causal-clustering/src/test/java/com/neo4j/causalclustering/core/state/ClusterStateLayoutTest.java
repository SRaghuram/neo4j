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

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.set;
import static org.neo4j.io.fs.FileUtils.path;

@ExtendWith( TestDirectoryExtension.class )
class ClusterStateLayoutTest
{
    private static final DatabaseId DATABASE_ID = new TestDatabaseIdRepository().get( "my_database" );

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
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "raft-id-state", "raft-id" ), layout.raftIdStateFile( DATABASE_ID ) );
    }

    @Test
    void shouldExposeIdAllocationStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "id-allocation-state" ), layout.idAllocationStateDirectory( DATABASE_ID ) );
    }

    @Test
    void shouldExposeLockTokenStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "lock-token-state" ), layout.lockTokenStateDirectory( DATABASE_ID ) );

    }

    @Test
    void shouldExposeLastFlushedStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "last-flushed-state" ), layout.lastFlushedStateDirectory( DATABASE_ID ) );
    }

    @Test
    void shouldExposeRaftMembershipStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "membership-state" ), layout.raftMembershipStateDirectory( DATABASE_ID ) );
    }

    @Test
    void shouldExposeRaftLogDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "raft-log" ), layout.raftLogDirectory( DATABASE_ID ) );
    }

    @Test
    void shouldExposeSessionTrackerDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "session-tracker-state" ), layout.sessionTrackerDirectory( DATABASE_ID ) );
    }

    @Test
    void shouldExposeRaftTermStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "term-state" ), layout.raftTermStateDirectory( DATABASE_ID ) );
    }

    @Test
    void shouldExposeRaftVoteStateDirectory()
    {
        assertEquals( path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "vote-state" ), layout.raftVoteStateDirectory( DATABASE_ID ) );
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
                path( dataDir, "cluster-state", "db", DATABASE_ID.name(),"raft-id-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "session-tracker-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "session-tracker-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "term-state" ),
                path( dataDir, "cluster-state", "db", DATABASE_ID.name(), "raft-log" )
        );

        Set<File> actual = layout.listGlobalAndDatabaseDirectories( DATABASE_ID, types::contains );

        assertEquals( expected, actual );
    }
}
