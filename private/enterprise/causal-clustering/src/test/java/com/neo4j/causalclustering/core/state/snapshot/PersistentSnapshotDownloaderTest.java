/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.dbms.ClusterInternalDbmsOperator.StoreCopyHandle;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;

import static com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.OPERATION_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PersistentSnapshotDownloaderTest
{
    private final SocketAddress fromAddress = new SocketAddress( "localhost", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = new CatchupAddressProvider.SingleAddressProvider( fromAddress );
    private final DatabasePanicker panicker = mock( DatabasePanicker.class );
    private final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
    private final NoPauseTimeoutStrategy backoffStrategy = new NoPauseTimeoutStrategy();
    private final CoreSnapshot snapshot = mock( CoreSnapshot.class );

    private CoreDownloader coreDownloader = mock( CoreDownloader.class );
    private CoreSnapshotService snapshotService = mock( CoreSnapshotService.class );
    private ReplicatedDatabaseEventService databaseEventService = mock( ReplicatedDatabaseEventService.class );

    private StoreDownloadContext downloadContext = mock( StoreDownloadContext.class );
    private StoreCopyHandle storeCopyHandle;

    private NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();

    private ReplicatedDatabaseEventDispatch databaseEventDispatch = mock( ReplicatedDatabaseEventDispatch.class );
    private SimpleTransactionIdStore txIdStore = new SimpleTransactionIdStore();

    private PersistentSnapshotDownloader createDownloader()
    {
        return new PersistentSnapshotDownloader( catchupAddressProvider, applicationProcess, coreDownloader, snapshotService, databaseEventService,
                downloadContext, mock( Log.class ), backoffStrategy, panicker, new Monitors() );
    }

    @BeforeEach
    void setUp()
    {
        when( downloadContext.databaseId() ).thenReturn( namedDatabaseId );

        storeCopyHandle = mock( StoreCopyHandle.class );
        when( downloadContext.stopForStoreCopy() ).thenReturn( storeCopyHandle );

        when( databaseEventService.getDatabaseEventDispatch( namedDatabaseId ) ).thenReturn( databaseEventDispatch );

        Database database = mock( Database.class );
        when( downloadContext.database() ).thenReturn( database );

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( txIdStore );

        when( database.getDependencyResolver() ).thenReturn( dependencies );
    }

    @Test
    void shouldHaltServicesDuringDownload() throws Throwable
    {
        // given
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.of( snapshot ) );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        persistentSnapshotDownloader.run();

        // then
        verify( panicker, never() ).panic( any() );

        InOrder inOrder = inOrder( applicationProcess, downloadContext, coreDownloader, storeCopyHandle );

        inOrder.verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        inOrder.verify( downloadContext ).stopForStoreCopy();

        inOrder.verify( coreDownloader ).downloadSnapshotAndStore( any(), any() );

        inOrder.verify( storeCopyHandle ).release();
        inOrder.verify( applicationProcess ).resumeApplier( OPERATION_NAME );

        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    void shouldDispatchDatabaseEventAfterStoreCopy() throws Throwable
    {
        // given
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.of( snapshot ) );
        when( storeCopyHandle.release() ).thenReturn( true );

        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();
        long txIdAfterDownload = 79;
        txIdStore.setLastCommittedAndClosedTransactionId( txIdAfterDownload, 0, 0, 0, 0 );

        // when
        persistentSnapshotDownloader.run();

        // then
        verify( panicker, never() ).panic( any() );

        InOrder inOrder = inOrder( storeCopyHandle, databaseEventDispatch );

        inOrder.verify( storeCopyHandle ).release();
        inOrder.verify( databaseEventDispatch ).fireStoreReplaced( txIdAfterDownload );
    }

    @Test
    void shouldResumeCommandApplicationProcessIfInterrupted() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.empty() );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        Thread thread = new Thread( persistentSnapshotDownloader );
        thread.start();
        awaitOneIteration( backoffStrategy );
        thread.interrupt();
        thread.join();

        // then
        verify( panicker, never() ).panic( any() );
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    void shouldResumeCommandApplicationProcessIfDownloaderIsStopped() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.empty() );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        Thread thread = new Thread( persistentSnapshotDownloader );
        thread.start();
        awaitOneIteration( backoffStrategy );
        persistentSnapshotDownloader.stop();
        thread.join();

        // then
        verify( panicker, never() ).panic( any() );
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    void shouldEventuallySucceed()
    {
        // given
        coreDownloader = new EventuallySuccessfulDownloader( 3 );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        persistentSnapshotDownloader.run();

        // then
        verify( panicker, never() ).panic( any() );
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
        assertEquals( 3, backoffStrategy.invocationCount() );
        assertTrue( persistentSnapshotDownloader.hasCompleted() );
    }

    @Test
    void shouldNotStartDownloadIfAlreadyCompleted() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.of( snapshot ) );

        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        persistentSnapshotDownloader.run();
        persistentSnapshotDownloader.run();

        // then
        verify( panicker, never() ).panic( any() );
        verify( coreDownloader ).downloadSnapshotAndStore( downloadContext, catchupAddressProvider );
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
    }

    @Test
    void shouldNotStartIfCurrentlyRunning() throws Exception
    {
        // given
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.empty() );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();
        Thread thread = new Thread( persistentSnapshotDownloader );

        // when
        thread.start();
        awaitOneIteration( backoffStrategy );
        persistentSnapshotDownloader.run();
        persistentSnapshotDownloader.stop();
        thread.join();

        // then
        verify( panicker, never() ).panic( any() );
        verify( applicationProcess ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess ).resumeApplier( OPERATION_NAME );
    }

    @Test
    void shouldPanicOnUnknownException() throws IOException, DatabaseShutdownException
    {
        // given
        RuntimeException runtimeException = new RuntimeException();
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenThrow( runtimeException );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();

        // when
        persistentSnapshotDownloader.run();

        // then
        verify( panicker ).panic( runtimeException );
    }

    @Test
    void shouldNotStartDatabaseServiceWhenStoppedDuringDownload() throws Throwable
    {
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.empty() );
        PersistentSnapshotDownloader persistentSnapshotDownloader = createDownloader();
        Thread thread = new Thread( persistentSnapshotDownloader );

        thread.start();
        awaitOneIteration( backoffStrategy );
        persistentSnapshotDownloader.stop();
        thread.join();

        verify( panicker, never() ).panic( any() );
        verify( downloadContext ).stopForStoreCopy();
        verify( storeCopyHandle, never() ).release();
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
            super( null, null, NullLogProvider.getInstance() );
            this.after = after;
        }

        @Override
        Optional<CoreSnapshot> downloadSnapshotAndStore( StoreDownloadContext context, CatchupAddressProvider addressProvider )
        {
            return after-- <= 0 ? Optional.of( snapshot ) : Optional.empty();
        }
    }
}
