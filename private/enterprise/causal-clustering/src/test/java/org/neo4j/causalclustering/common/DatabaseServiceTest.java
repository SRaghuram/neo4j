/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.common;

import org.junit.Test;
import org.mockito.InOrder;

import java.time.Clock;

import org.neo4j.causalclustering.helpers.FakeJobScheduler;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.reset;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class DatabaseServiceTest
{

    @Test
    public void availabilityGuardRaisedOnCreation()
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );
        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DataSourceManager.class )  );

        assertNotNull( databaseService );
        assertFalse( guard.isAvailable() );
    }

    @Test
    public void availabilityGuardDroppedOnStart() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DataSourceManager.class ) );
        assertFalse( guard.isAvailable() );

        databaseService.start();
        assertTrue( guard.isAvailable() );
    }

    @Test
    public void availabilityGuardRaisedOnStop() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DataSourceManager.class ) );
        assertFalse( guard.isAvailable() );

        databaseService.start();
        assertTrue( guard.isAvailable() );

        databaseService.stop();
        assertFalse( guard.isAvailable() );
        assertThat( guard.describe(), containsString( DefaultDatabaseService.STOPPED_MSG ) );
    }

    @Test
    public void availabilityGuardRaisedOnStopForStoreCopy() throws Throwable
    {
        DatabaseAvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        DatabaseService databaseService = newLocalDatabaseService( guard, mock( DataSourceManager.class ) );
        assertFalse( guard.isAvailable() );

        databaseService.start();
        assertTrue( guard.isAvailable() );

        databaseService.stopForStoreCopy();
        assertFalse( guard.isAvailable() );
        assertThat( guard.describe(), containsString( DefaultDatabaseService.COPYING_STORE_MSG ) );
    }

    @Test
    public void availabilityGuardRaisedBeforeDataSourceManagerIsStopped() throws Throwable
    {
        AvailabilityGuard guard = mock( DatabaseAvailabilityGuard.class );
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );

        DatabaseService databaseService = newLocalDatabaseService( guard, dataSourceManager );
        databaseService.stop();

        InOrder inOrder = inOrder( guard, dataSourceManager );
        // guard should be raised twice - once during construction and once during stop
        inOrder.verify( guard, times( 2 ) ).require( any() );
        inOrder.verify( dataSourceManager ).stop();
    }

    @Test
    public void availabilityGuardRaisedBeforeDataSourceManagerIsStoppedForStoreCopy() throws Throwable
    {
        AvailabilityGuard guard = mock( DatabaseAvailabilityGuard.class );
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );

        DatabaseService databaseService = newLocalDatabaseService( guard, dataSourceManager );
        databaseService.stopForStoreCopy();

        InOrder inOrder = inOrder( guard, dataSourceManager );
        // guard should be raised twice - once during construction and once during stop
        inOrder.verify( guard, times( 2 ) ).require( any() );
        inOrder.verify( dataSourceManager ).stop();
    }

    @Test
    public void doNotRestartServicesIfAlreadyStarted() throws Throwable
    {
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        DatabaseService databaseService = newLocalDatabaseService( newAvailabilityGuard(), dataSourceManager );

        databaseService.start();

        verify( dataSourceManager ).start();
        reset( dataSourceManager );

        databaseService.start();
        databaseService.start();

        verify( dataSourceManager, never() ).start();
    }

    protected DatabaseAvailabilityGuard newAvailabilityGuard()
    {
        return new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, Clock.systemUTC(), NullLog.getInstance() );
    }

    protected DatabaseService newLocalDatabaseService( AvailabilityGuard availabilityGuard, DataSourceManager dataSourceManager )
    {
        return new DefaultDatabaseService<>( StubLocalDatabase::new, dataSourceManager, mock( StoreLayout.class ), availabilityGuard,
                () -> mock( DatabaseHealth.class ), mock( FileSystemAbstraction.class ), mock( PageCache.class ),
                new FakeJobScheduler(), NullLogProvider.getInstance(), Config.defaults() );
    }
}
