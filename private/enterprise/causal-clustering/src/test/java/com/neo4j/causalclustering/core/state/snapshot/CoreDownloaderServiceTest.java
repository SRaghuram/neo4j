/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.ReplicatedDatabaseEventService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.util.CountingJobScheduler;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.DOWNLOAD_SNAPSHOT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

class CoreDownloaderServiceTest
{
    private final SocketAddress someMemberAddress = new SocketAddress( "localhost", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = new CatchupAddressProvider.SingleAddressProvider( someMemberAddress );
    private final CoreDownloader coreDownloader = mock( CoreDownloader.class );
    private final CoreSnapshotService snapshotService = mock( CoreSnapshotService.class );
    private final ReplicatedDatabaseEventService databaseEventService = mock( ReplicatedDatabaseEventService.class );
    private final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
    private final StubClusteredDatabaseManager databaseManager = new StubClusteredDatabaseManager();
    private final LogProvider logProvider = NullLogProvider.getInstance();

    private JobScheduler centralJobScheduler;
    private final DatabasePanicker panicker = mock( DatabasePanicker.class );
    private final DatabaseStartAborter databaseStartAborter = mock( DatabaseStartAborter.class );
    private StoreDownloadContext downloadContext = mock( StoreDownloadContext.class );

    @BeforeEach
    void create()
    {
        centralJobScheduler = createInitialisedScheduler();
        databaseManager.givenDatabaseWithConfig()
                       .withDatabaseId( new TestDatabaseIdRepository().defaultDatabase() )
                       .register();
        when( downloadContext.databaseId() ).thenReturn( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) );
    }

    private CoreDownloaderService createDownloader()
    {
        return new CoreDownloaderService( centralJobScheduler, coreDownloader, downloadContext, snapshotService, databaseEventService, applicationProcess,
                logProvider, new NoPauseTimeoutStrategy(), panicker, new Monitors(), databaseStartAborter );
    }

    @AfterEach
    void shutdown() throws Throwable
    {
        centralJobScheduler.shutdown();
    }

    @Test
    void shouldRunPersistentDownloader() throws Exception
    {
        when( coreDownloader.downloadSnapshotAndStore( any(), any() ) ).thenReturn( Optional.of( mock( CoreSnapshot.class ) ) );

        CoreDownloaderService coreDownloaderService = createDownloader();
        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        waitForApplierToResume( applicationProcess );

        verify( applicationProcess ).pauseApplier( DOWNLOAD_SNAPSHOT );
        verify( applicationProcess ).resumeApplier( DOWNLOAD_SNAPSHOT );
        verify( coreDownloader ).downloadSnapshotAndStore( any(), any() );
    }

    @Test
    void shouldOnlyScheduleOnePersistentDownloaderTaskAtTheTime() throws InterruptedException
    {
        AtomicInteger schedules = new AtomicInteger();
        CountingJobScheduler countingJobScheduler = new CountingJobScheduler( schedules, centralJobScheduler );
        Semaphore blockDownloader = new Semaphore( 0 );
        CoreDownloader coreDownloader = new BlockingCoreDownloader( blockDownloader );

        CoreDownloaderService coreDownloaderService = new CoreDownloaderService( countingJobScheduler, coreDownloader, downloadContext, snapshotService,
                databaseEventService, applicationProcess, logProvider, new NoPauseTimeoutStrategy(), panicker, new Monitors(), databaseStartAborter );

        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        Thread.sleep( 50 );
        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        coreDownloaderService.scheduleDownload( catchupAddressProvider );

        Assertions.assertEquals( 1, schedules.get() );
        blockDownloader.release();
    }

    static class BlockingCoreDownloader extends CoreDownloader
    {
        private final Semaphore semaphore;

        BlockingCoreDownloader( Semaphore semaphore )
        {
            super( null, null, NullLogProvider.getInstance() );
            this.semaphore = semaphore;
        }

        @Override
        Optional<CoreSnapshot> downloadSnapshotAndStore( StoreDownloadContext context, CatchupAddressProvider addressProvider )
        {
            semaphore.acquireUninterruptibly();
            return Optional.of( mock( CoreSnapshot.class ) );
        }
    }

    private void waitForApplierToResume( CommandApplicationProcess applicationProcess ) throws TimeoutException
    {
        Predicates.await( () ->
        {
            try
            {
                verify( applicationProcess ).resumeApplier( DOWNLOAD_SNAPSHOT );
                return true;
            }
            catch ( Throwable t )
            {
                return false;
            }
        }, 20, TimeUnit.SECONDS );
    }
}
