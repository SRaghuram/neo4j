/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.OPERATION_NAME;

public class PersistentSnapshotDownloaderTest
{
    private final AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "localhost", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress(
            fromAddress );
    private final Panicker panicker = mock( Panicker.class );
    private final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
    private final NoPauseTimeoutStrategy backoffStrategy = new NoPauseTimeoutStrategy();
    private final CoreSnapshot snapshot = mock( CoreSnapshot.class );

    private CoreDownloader coreDownloader = mock( CoreDownloader.class );
    private CoreSnapshotService snapshotService = mock( CoreSnapshotService.class );

    private final DatabaseService databaseService = mock( DatabaseService.class );
    private final Suspendable auxiliaryServices = mock( Suspendable.class );

    private PersistentSnapshotDownloader createDownloader()
    {
        return new PersistentSnapshotDownloader( catchupAddressProvider, applicationProcess, auxiliaryServices, databaseService,
                coreDownloader, snapshotService, mock( Log.class ), backoffStrategy, panicker, new Monitors() );
    }

    @Test
    public void shouldHaltServicesDuringDownload() throws Throwable
    {
        // given
        when( coreDownloader.downloadSnapshotAndStores( any() ) ).thenReturn( Optional.of( snapshot ) );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        persistentSnapshotDownloader.run();

        // then
        InOrder inOrder = inOrder( applicationProcess, databaseService, auxiliaryServices, coreDownloader );

        inOrder.verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        inOrder.verify( auxiliaryServices ).disable();
        inOrder.verify( databaseService ).stopForStoreCopy();

        inOrder.verify( coreDownloader ).downloadSnapshotAndStores( any() );

        inOrder.verify( databaseService ).start();
        inOrder.verify( auxiliaryServices ).enable();
        inOrder.verify( applicationProcess ).resumeApplier( OPERATION_NAME );

        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldResumeCommandApplicationProcessIfInterrupted() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStores( any() ) ).thenReturn( Optional.empty() );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        Thread thread = new Thread( persistentSnapshotDownloader );
        thread.start();
        awaitOneIteration( backoffStrategy );
        thread.interrupt();
        thread.join();

        // then
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldResumeCommandApplicationProcessIfDownloaderIsStopped() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStores( any() ) ).thenReturn( Optional.empty() );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        Thread thread = new Thread( persistentSnapshotDownloader );
        thread.start();
        awaitOneIteration( backoffStrategy );
        persistentSnapshotDownloader.stop();
        thread.join();

        // then
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldEventuallySucceed()
    {
        // given
        coreDownloader = new EventuallySuccessfulDownloader( 3 );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        persistentSnapshotDownloader.run();

        // then
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
        assertEquals( 3, backoffStrategy.invocationCount() );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    public void shouldNotStartDownloadIfAlreadyCompleted() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStores( any() ) ).thenReturn( Optional.of( snapshot ) );

        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        persistentSnapshotDownloader.run();
        persistentSnapshotDownloader.run();

        // then
        verify( coreDownloader ).downloadSnapshotAndStores( catchupAddressProvider );
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
    }

    @Test
    public void shouldNotStartIfCurrentlyRunning() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStores( any() ) ).thenReturn( Optional.empty() );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();
        Thread thread = new Thread( persistentSnapshotDownloader );

        // when
        thread.start();
        awaitOneIteration( backoffStrategy );
        persistentSnapshotDownloader.run();
        persistentSnapshotDownloader.stop();
        thread.join();

        // then
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
    }

    @Test
    public void shoulPanicOnUnknonExcpetion() throws IOException, DatabaseShutdownException
    {
        // given
        RuntimeException runtimeException = new RuntimeException();
        when( coreDownloader.downloadSnapshotAndStores( any() ) ).thenThrow( runtimeException );
        final Log log = mock( Log.class );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();
        // when
        persistentSnapshotDownloader.run();

        // then
        verify( panicker ).panic( runtimeException );
    }

    private void awaitOneIteration( NoPauseTimeoutStrategy backoffStrategy ) throws TimeoutException
    {
        Predicates.await( () -> backoffStrategy.invocationCount() > 0, 2, TimeUnit.SECONDS );
    }

    private class EventuallySuccessfulDownloader extends CoreDownloader
    {
        private int after;

        private EventuallySuccessfulDownloader( int after )
        {
            super( null, null, null, NullLogProvider.getInstance() );
            this.after = after;
        }

        @Override
        Optional<CoreSnapshot> downloadSnapshotAndStores( CatchupAddressProvider addressProvider )
        {
            return after-- <= 0 ? Optional.of( snapshot ) : Optional.empty();
        }
    }
}
