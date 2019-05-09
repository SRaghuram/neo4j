/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( LifeExtension.class )
class ClusteredDatabaseManagerTest
{

    @Inject
    private LifeSupport lifeSupport;

    @Test
    void availabilityGuardRaisedOnCreation()
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );
        ClusteredDatabaseManager clusteredDatabaseManager = newDatabaseManager( guard );

        assertNotNull( clusteredDatabaseManager );
        assertFalse( guard.isAvailable() );
    }

    @Test
    void availabilityGuardDroppedOnStart() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        ClusteredDatabaseManager clusteredDatabaseManager = newDatabaseManager( guard );
        assertFalse( guard.isAvailable() );

        clusteredDatabaseManager.start();
        assertTrue( guard.isAvailable() );
    }

    @Test
    void availabilityGuardRaisedOnStop() throws Throwable
    {
        AvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        ClusteredDatabaseManager clusteredDatabaseManager = newDatabaseManager( guard );
        assertFalse( guard.isAvailable() );

        clusteredDatabaseManager.start();
        assertTrue( guard.isAvailable() );

        clusteredDatabaseManager.stop();
        assertFalse( guard.isAvailable() );
        assertThat( guard.describe(), containsString( ClusteredMultiDatabaseManager.STOPPED_MSG ) );
    }

    @Test
    void availabilityGuardRaisedOnStopForStoreCopy() throws Throwable
    {
        DatabaseAvailabilityGuard guard = newAvailabilityGuard();
        assertTrue( guard.isAvailable() );

        ClusteredDatabaseManager clusteredDatabaseManager = newDatabaseManager( guard );
        assertFalse( guard.isAvailable() );

        clusteredDatabaseManager.start();
        assertTrue( guard.isAvailable() );

        clusteredDatabaseManager.stopForStoreCopy();
        assertFalse( guard.isAvailable() );
        assertThat( guard.describe(), containsString( ClusteredMultiDatabaseManager.COPYING_STORE_MSG ) );
    }

    //TODO: uncomment tests when they're rewritten for new databaseManager
//    @Test
//    void availabilityGuardRaisedBeforeDatabasesAreStopped() throws Throwable
//    {
//        AvailabilityGuard guard = mock( DatabaseAvailabilityGuard.class );
//        ClusteredDatabaseManager<StubClusteredDatabaseContext> clusteredDatabaseManager = newDatabaseManager( guard );
//        clusteredDatabaseManager.createDatabase(  )
//        clusteredDatabaseManager.stop();
//
//        InOrder inOrder = inOrder( guard, databaseManager );
//        // guard should be raised twice - once during construction and once during stop
//        inOrder.verify( guard, times( 2 ) ).require( any() );
//        inOrder.verify( clusteredDatabaseManager ).stop();
//    }
//
//    @Test
//    void availabilityGuardRaisedBeforeDataSourceManagerIsStoppedForStoreCopy() throws Throwable
//    {
//        AvailabilityGuard guard = mock( DatabaseAvailabilityGuard.class );
//        DatabaseManager<StubClusteredDatabaseContext> databaseManager = mock( DatabaseManager.class );
//
//        ClusteredDatabaseManager<StubClusteredDatabaseContext> clusteredDatabaseManager = newDatabaseManager( guard, databaseManager );
//        clusteredDatabaseManager.stopForStoreCopy();
//
//        InOrder inOrder = inOrder( guard, databaseManager );
//        // guard should be raised twice - once during construction and once during stop
//        inOrder.verify( guard, times( 2 ) ).require( any() );
//        inOrder.verify( databaseManager ).stop();
//    }
//
//    @Test
//    void doNotRestartServicesIfAlreadyStarted() throws Throwable
//    {
//        DatabaseManager<StubClusteredDatabaseContext> databaseManager = mock( DatabaseManager.class );
//        ClusteredDatabaseManager<StubClusteredDatabaseContext> clusteredDatabaseManager = newDatabaseManager( newAvailabilityGuard(), databaseManager );
//
//        clusteredDatabaseManager.start();
//
//        verify( databaseManager ).start();
//        reset( databaseManager );
//
//        clusteredDatabaseManager.start();
//        clusteredDatabaseManager.start();
//
//        verify( databaseManager, never() ).start();
//    }

    private DatabaseAvailabilityGuard newAvailabilityGuard()
    {
        DatabaseId databaseId = new DatabaseId( DEFAULT_DATABASE_NAME );
        DatabaseAvailabilityGuard availabilityGuard = new DatabaseAvailabilityGuard( databaseId, Clock.systemUTC(), NullLog.getInstance(), 0,
                        mock( CompositeDatabaseAvailabilityGuard.class ) );
        lifeSupport.add( availabilityGuard );
        return availabilityGuard;
    }

    private static ClusteredDatabaseManager newDatabaseManager( AvailabilityGuard availabilityGuard )
    {
        return new ClusteredMultiDatabaseManager( mock( GlobalModule.class ), mock( AbstractEditionModule.class ), mock( Log.class ),
                StubClusteredDatabaseContext::new, mock( CatchupComponentsFactory.class ),
                mock( FileSystemAbstraction.class ), mock( PageCache.class ), NullLogProvider.getInstance(), Config.defaults(),
                mock( DatabaseHealth.class ), availabilityGuard );
    }
}
