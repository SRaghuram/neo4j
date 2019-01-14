/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.helper.Suspendable;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.impl.util.CountingJobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader.OPERATION_NAME;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class CoreDownloaderServiceTest
{
    private final AdvertisedSocketAddress someMemberAddress = new AdvertisedSocketAddress( "localhost", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( someMemberAddress );
    private final DatabaseHealth dbHealth = mock( DatabaseHealth.class );
    private final CoreDownloader coreDownloader = mock( CoreDownloader.class );
    private final CoreSnapshotService snapshotService = mock( CoreSnapshotService.class );
    private final CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );
    private final Suspendable suspendedServices = mock( Suspendable.class );
    private final DatabaseService databases = mock( DatabaseService.class );
    private final LogProvider logProvider = NullLogProvider.getInstance();

    private JobScheduler centralJobScheduler;

    @Before
    public void create()
    {
        centralJobScheduler = createInitialisedScheduler();
    }

    private CoreDownloaderService createDownloader()
    {
        return new CoreDownloaderService( centralJobScheduler, coreDownloader, snapshotService, suspendedServices, databases, applicationProcess,
                logProvider, new NoPauseTimeoutStrategy(), () -> dbHealth, new Monitors() );
    }

    @After
    public void shutdown() throws Throwable
    {
        centralJobScheduler.shutdown();
    }

    @Test
    public void shouldRunPersistentDownloader() throws Exception
    {
        when( coreDownloader.downloadSnapshotAndStores( any() ) ).thenReturn( Optional.of( mock( CoreSnapshot.class ) ) );

        CoreDownloaderService coreDownloaderService = createDownloader();
        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        waitForApplierToResume( applicationProcess );

        verify( applicationProcess, times( 1 ) ).pauseApplier( OPERATION_NAME );
        verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
        verify( coreDownloader, times( 1 ) ).downloadSnapshotAndStores( any() );
    }

    @Test
    public void shouldOnlyScheduleOnePersistentDownloaderTaskAtTheTime() throws InterruptedException
    {
        AtomicInteger schedules = new AtomicInteger();
        CountingJobScheduler countingJobScheduler = new CountingJobScheduler( schedules, centralJobScheduler );
        Semaphore blockDownloader = new Semaphore( 0 );
        CoreDownloader coreDownloader = new BlockingCoreDownloader( blockDownloader );

        CoreDownloaderService coreDownloaderService = new CoreDownloaderService( countingJobScheduler, coreDownloader, snapshotService,
                suspendedServices, databases, applicationProcess, logProvider, new NoPauseTimeoutStrategy(), () -> dbHealth, new Monitors() );

        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        Thread.sleep( 50 );
        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        coreDownloaderService.scheduleDownload( catchupAddressProvider );
        coreDownloaderService.scheduleDownload( catchupAddressProvider );

        assertEquals( 1, schedules.get() );
        blockDownloader.release();
    }

    static class BlockingCoreDownloader extends CoreDownloader
    {
        private final Semaphore semaphore;

        BlockingCoreDownloader( Semaphore semaphore )
        {
            super( null, null, null, NullLogProvider.getInstance() );
            this.semaphore = semaphore;
        }

        @Override
        Optional<CoreSnapshot> downloadSnapshotAndStores( CatchupAddressProvider addressProvider )
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
                verify( applicationProcess, times( 1 ) ).resumeApplier( OPERATION_NAME );
                return true;
            }
            catch ( Throwable t )
            {
                return false;
            }
        }, 20, TimeUnit.SECONDS );
    }
}
