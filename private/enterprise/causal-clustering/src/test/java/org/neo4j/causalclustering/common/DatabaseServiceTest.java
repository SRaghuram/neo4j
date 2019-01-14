/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.common;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Clock;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class DatabaseServiceTest
{

    @Test
    void availabilityGuardRaisedOnCreation()
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );
        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DatabaseManager.class )  );

        assertNotNull( databaseService );
        assertFalse( guard.isAvailable() );
    }

    @Test
    void availabilityGuardDroppedOnStart() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DatabaseManager.class ) );
        assertFalse( guard.isAvailable() );

        databaseService.start();
        assertTrue( guard.isAvailable() );
    }

    @Test
    void availabilityGuardRaisedOnStop() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DatabaseManager.class ) );
        assertFalse( guard.isAvailable() );

        databaseService.start();
        assertTrue( guard.isAvailable() );

        databaseService.stop();
        assertFalse( guard.isAvailable() );
        assertThat( guard.describe(), containsString( DefaultDatabaseService.STOPPED_MSG ) );
    }

    @Test
    void availabilityGuardRaisedOnStopForStoreCopy() throws Throwable
    {
        DatabaseAvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DatabaseManager.class ) );
        assertFalse( guard.isAvailable() );

        databaseService.start();
        assertTrue( guard.isAvailable() );

        databaseService.stopForStoreCopy();
        assertFalse( guard.isAvailable() );
        assertThat( guard.describe(), containsString( DefaultDatabaseService.COPYING_STORE_MSG ) );
    }

    @Test
    void availabilityGuardRaisedBeforeDataSourceManagerIsStopped() throws Throwable
    {
        AvailabilityGuard guard = mock( DatabaseAvailabilityGuard.class );
        DatabaseManager databaseManager = mock( DatabaseManager.class );

        DatabaseService databaseService = newLocalDatabaseService( guard, databaseManager );
        databaseService.stop();

        InOrder inOrder = inOrder( guard, databaseManager );
        // guard should be raised twice - once during construction and once during stop
        inOrder.verify( guard, times( 2 ) ).require( any() );
        inOrder.verify( databaseManager ).stop();
    }

    @Test
    void availabilityGuardRaisedBeforeDataSourceManagerIsStoppedForStoreCopy() throws Throwable
    {
        AvailabilityGuard guard = mock( DatabaseAvailabilityGuard.class );
        DatabaseManager databaseManager = mock( DatabaseManager.class );

        DatabaseService databaseService = newLocalDatabaseService( guard, databaseManager );
        databaseService.stopForStoreCopy();

        InOrder inOrder = inOrder( guard, databaseManager );
        // guard should be raised twice - once during construction and once during stop
        inOrder.verify( guard, times( 2 ) ).require( any() );
        inOrder.verify( databaseManager ).stop();
    }

    @Test
    void doNotRestartServicesIfAlreadyStarted() throws Throwable
    {
        DatabaseManager databaseManager = mock( DatabaseManager.class );
        DatabaseService databaseService = newLocalDatabaseService( newAvailabilityGuard(), databaseManager );

        databaseService.start();

        verify( databaseManager ).start();
        reset( databaseManager );

        databaseService.start();
        databaseService.start();

        verify( databaseManager, never() ).start();
    }

    DatabaseAvailabilityGuard newAvailabilityGuard()
    {
        return new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, Clock.systemUTC(), NullLog.getInstance() );
    }

    DatabaseService newLocalDatabaseService( AvailabilityGuard availabilityGuard, DatabaseManager databaseManager )
    {
        return new DefaultDatabaseService<>( StubLocalDatabase::new, () -> databaseManager, mock( StoreLayout.class ), availabilityGuard,
                () -> mock( DatabaseHealth.class ), mock( FileSystemAbstraction.class ), mock( PageCache.class ),
                NullLogProvider.getInstance(), Config.defaults() );
    }
}
