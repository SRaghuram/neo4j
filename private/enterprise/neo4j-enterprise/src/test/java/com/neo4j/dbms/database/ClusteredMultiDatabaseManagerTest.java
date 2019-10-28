/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.common.ClusteredDatabaseLife;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.scheduler.CallingThreadJobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ClusteredMultiDatabaseManagerTest
{

    @Test
    void shouldRecreateDatabaseContextOnRestart() throws Exception
    {
        // given
        DatabaseId dbId = TestDatabaseIdRepository.randomDatabaseId();
        StubClusteredMultiDatabaseManager manager = new StubClusteredMultiDatabaseManager();
        ClusteredDatabaseContext ctx = manager.createDatabaseContext( dbId );

        StubClusteredDatabaseLife dbLife = (StubClusteredDatabaseLife) ctx.clusteredDatabaseLife();

        // when
        manager.testStartDatabase( dbId, ctx );
        manager.testStartDatabase( dbId, ctx );

        // then
        verify( dbLife.startStopTracker, times( 1 ) ).start();
        verify( dbLife.startStopTracker, times( 1 ) ).stop();
        verifyNoMoreInteractions( dbLife.startStopTracker );
        assertNotSame( ctx, manager.mostRecentContext(), "The most recent context object should be different after a restart" );
        assertEquals( ctx.databaseId(), dbId, "The new context should have the same database Id" );
    }

    private class StubClusteredMultiDatabaseManager extends ClusteredMultiDatabaseManager
    {
        private ClusteredDatabaseContext context;

        StubClusteredMultiDatabaseManager()
        {
            super( StubMultiDatabaseManager.mockGlobalModule( new CallingThreadJobScheduler() ), null, null, null, null,
                    NullLogProvider.getInstance(), Config.defaults(), null );
        }

        void testStartDatabase( DatabaseId databaseId, ClusteredDatabaseContext databaseContext ) throws Exception
        {
            super.startDatabase( databaseId, databaseContext );
        }

        ClusteredDatabaseContext mostRecentContext()
        {
            return context;
        }

        @Override
        protected ClusteredDatabaseContext createDatabaseContext( DatabaseId databaseId )
        {
            var dbCtx = mock( ClusteredDatabaseContext.class );
            when( dbCtx.databaseId() ).thenReturn( databaseId );
            StartStop startStopTracker = mock( StartStop.class );
            ClusteredDatabaseLife dbLife = new StubClusteredDatabaseLife( startStopTracker );
            when( dbCtx.clusteredDatabaseLife() ).thenReturn( dbLife );
            context = dbCtx;
            return dbCtx;
        }

        /* UNUSED NO-OP OVERRIDES */
        @Override
        public void cleanupClusterState( String databaseName )
        {
        }

        @Override
        public void initialiseSystemDatabase()
        {
        }

        @Override
        public void initialiseDefaultDatabase()
        {
        }

        @Override
        public Optional<ClusteredDatabaseContext> getDatabaseContext( String databaseName )
        {
            return Optional.empty();
        }
    }

    private class StubClusteredDatabaseLife extends ClusteredDatabaseLife
    {
        private final StartStop startStopTracker;

        StubClusteredDatabaseLife( StartStop startStopTracker )
        {
            this.startStopTracker = startStopTracker;
        }

        @Override
        protected void start0()
        {
            startStopTracker.start();
        }

        @Override
        protected void stop0()
        {
            startStopTracker.stop();
        }
    }

    private interface StartStop
    {
        void start();
        void stop();
    }
}
