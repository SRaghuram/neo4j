/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import com.neo4j.causalclustering.helper.TimeoutStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.String.format;

class ReadReplicaDatabaseLife extends SafeLifecycle
{
    private final Lifecycle catchupProcess;
    private final Log debugLog;
    private final Log userLog;
    private final TimeoutStrategy syncRetryStrategy;
    private final UpstreamDatabaseStrategySelector selectionStrategy;
    private final TopologyService topologyService;

    private final Supplier<CatchupComponents> catchupComponentsSupplier;
    private final ReadReplicaDatabaseContext databaseContext;

    ReadReplicaDatabaseLife( ReadReplicaDatabaseContext databaseContext, Lifecycle catchupProcess, UpstreamDatabaseStrategySelector selectionStrategy,
            LogProvider debugLogProvider, LogProvider userLogProvider, TopologyService topologyService, Supplier<CatchupComponents> catchupComponentsSupplier )
    {
        this.databaseContext = databaseContext;
        this.catchupComponentsSupplier = catchupComponentsSupplier;
        this.catchupProcess = catchupProcess;
        this.selectionStrategy = selectionStrategy;
        this.syncRetryStrategy = new ExponentialBackoffStrategy( 1, 30, TimeUnit.SECONDS );
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.topologyService = topologyService;
    }

    @Override
    public void init0() throws Exception
    {
        databaseContext.database().init();
        catchupProcess.init();
    }

    @Override
    public void start0() throws Exception
    {
        TimeoutStrategy.Timeout syncRetryWaitPeriod = syncRetryStrategy.newTimeout();
        boolean synced = false;
        while ( !synced )
        {
            try
            {
                debugLog.info( "Syncing db: %s", databaseContext.database().getDatabaseId() );
                synced = doSyncStoreCopyWithUpstream( databaseContext );
                if ( synced )
                {
                    debugLog.info( "Successfully synced db: %s", databaseContext.database().getDatabaseId() );
                }
                else
                {
                    Thread.sleep( syncRetryWaitPeriod.getMillis() );
                    syncRetryWaitPeriod.increment();
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                userLog.error( "Interrupted while trying to start read replica" );
                throw new RuntimeException( e );
            }
            catch ( Exception e )
            {
                debugLog.error( "Unexpected error when syncing stores", e );
                throw new RuntimeException( e );
            }
        }

        databaseContext.database().start();
        catchupProcess.start();
    }

    private boolean doSyncStoreCopyWithUpstream( ReadReplicaDatabaseContext databaseContext )
    {
        MemberId source;
        try
        {
            source = selectionStrategy.bestUpstreamMemberForDatabase( databaseContext.databaseId() );
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            debugLog.warn( "Unable to find upstream member for database " + databaseContext.databaseId().name() );
            return false;
        }

        try
        {
            syncStoreWithUpstream( databaseContext, source );
            return true;
        }
        catch ( TopologyLookupException e )
        {
            debugLog.warn( "Unable to get address of %s", source );
            return false;
        }
        catch ( StoreIdDownloadFailedException e )
        {
            debugLog.warn( "Unable to get store ID from %s", source );
            return false;
        }
        catch ( StoreCopyFailedException e )
        {
            debugLog.warn( "Unable to copy store files from %s", source );
            return false;
        }
        catch ( DatabaseShutdownException | IOException e )
        {
            debugLog.warn( format( "Syncing of stores failed unexpectedly from %s", source ), e );
            return false;
        }
    }

    private void syncStoreWithUpstream( ReadReplicaDatabaseContext databaseContext, MemberId source )
            throws IOException, StoreIdDownloadFailedException, StoreCopyFailedException, TopologyLookupException, DatabaseShutdownException
    {
        CatchupComponents catchupComponents = catchupComponentsSupplier.get();

        if ( databaseContext.isEmpty() )
        {
            debugLog.info( "Local database is empty, attempting to replace with copy from upstream server %s", source );

            debugLog.info( "Finding store ID of upstream server %s", source );
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source );
            StoreId storeId = catchupComponents.remoteStore().getStoreId( fromAddress );

            debugLog.info( "Copying store from upstream server %s", source );
            databaseContext.delete();
            catchupComponents.storeCopyProcess().replaceWithStoreFrom( new SingleAddressProvider( fromAddress ), storeId );

            debugLog.info( "Restarting local database after copy.", source );
        }
        else
        {
            ensureStoreIsPresentAt( databaseContext, catchupComponents.remoteStore(), source );
        }
    }

    private void ensureStoreIsPresentAt( ReadReplicaDatabaseContext databaseContext, RemoteStore remoteStore, MemberId upstream )
            throws StoreIdDownloadFailedException, TopologyLookupException
    {
        StoreId localStoreId = databaseContext.storeId();
        AdvertisedSocketAddress advertisedSocketAddress = topologyService.findCatchupAddress( upstream );
        StoreId remoteStoreId = remoteStore.getStoreId( advertisedSocketAddress );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException(
                    format( "This read replica cannot join the cluster. " + "The local version of database %s is not empty and has a mismatching storeId: " +
                            "expected %s actual %s.", databaseContext.databaseId().name(), remoteStoreId, localStoreId ) );
        }
    }

    @Override
    public void stop0() throws Exception
    {
        catchupProcess.stop();
        databaseContext.database().stop();
    }

    @Override
    public void shutdown0() throws Exception
    {
        catchupProcess.shutdown();
        databaseContext.database().shutdown();
    }
}
