/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import static java.lang.String.format;

class ReadReplicaBootstrap
{
    private final Log debugLog;
    private final Log userLog;
    private final TimeoutStrategy syncRetryStrategy;
    private final UpstreamDatabaseStrategySelector selectionStrategy;
    private final DatabaseStartAborter databaseStartAborter;
    private final ClusterInternalDbmsOperator internalOperator;
    private final TopologyService topologyService;
    private final Supplier<CatchupComponents> catchupComponentsSupplier;
    private final ReadReplicaDatabaseContext databaseContext;
    private final LastTxMonitor lastTxIdMonitor;

    ReadReplicaBootstrap( ReadReplicaDatabaseContext databaseContext, UpstreamDatabaseStrategySelector selectionStrategy, LogProvider debugLogProvider,
                          LogProvider userLogProvider, TopologyService topologyService, Supplier<CatchupComponents> catchupComponentsSupplier,
                          ClusterInternalDbmsOperator internalOperator, DatabaseStartAborter databaseStartAborter, TimeoutStrategy syncRetryStrategy,
                          CommandIndexTracker commandIndexTracker )
    {
        this.databaseContext = databaseContext;
        this.catchupComponentsSupplier = catchupComponentsSupplier;
        this.selectionStrategy = selectionStrategy;
        this.databaseStartAborter = databaseStartAborter;
        this.syncRetryStrategy = syncRetryStrategy;
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.topologyService = topologyService;
        this.internalOperator = internalOperator;
        this.lastTxIdMonitor = new LastTxMonitor( commandIndexTracker );
    }

    public void perform() throws Exception
    {
        var bootstrapHandle = internalOperator.bootstrap( databaseContext.databaseId() );
        boolean shouldAbort = false;
        try
        {
            var synced = false;
            TimeoutStrategy.Timeout syncRetryWaitPeriod = syncRetryStrategy.newTimeout();

            while ( !( synced || shouldAbort ) )
            {
                try
                {
                    for ( var upstream : selectUpstreams(  databaseContext ) )
                    {
                        synced = doSyncStoreCopyWithUpstream( databaseContext, upstream );

                        if ( synced )
                        {
                            break;
                        }
                    }

                    if ( !synced )
                    {
                        Thread.sleep( syncRetryWaitPeriod.getMillis() );
                        syncRetryWaitPeriod.increment();
                    }
                    shouldAbort = databaseStartAborter.shouldAbort( databaseContext.databaseId() );
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
        }
        finally
        {
            databaseStartAborter.started( databaseContext.databaseId() );
            bootstrapHandle.release();
        }

        if ( shouldAbort )
        {
            throw new DatabaseStartAbortedException( databaseContext.databaseId() );
        }
    }

    private Collection<ServerId> selectUpstreams( ReadReplicaDatabaseContext databaseContext )
    {
        try
        {
            return selectionStrategy.bestUpstreamServersForDatabase( databaseContext.databaseId() );
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            debugLog.warn( "Unable to find upstream member for " + databaseContext.databaseId() );
        }
        return List.of();
    }

    private boolean doSyncStoreCopyWithUpstream( ReadReplicaDatabaseContext databaseContext, ServerId source )
    {
        try
        {
            debugLog.info( "Syncing db: %s", databaseContext.databaseId() );
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

    private void syncStoreWithUpstream( ReadReplicaDatabaseContext databaseContext, ServerId source )
            throws IOException, StoreIdDownloadFailedException, StoreCopyFailedException, TopologyLookupException, DatabaseShutdownException
    {
        CatchupComponentsRepository.CatchupComponents catchupComponents = catchupComponentsSupplier.get();

        if ( databaseContext.isEmpty() )
        {
            debugLog.info( "Local database is empty, attempting to replace with copy from upstream server %s", source );

            debugLog.info( "Finding store ID of upstream server %s", source );
            SocketAddress fromAddress = topologyService.lookupCatchupAddress( source );
            StoreId storeId = catchupComponents.remoteStore().getStoreId( fromAddress );

            debugLog.info( "Copying store from upstream server %s", source );
            databaseContext.delete();
            databaseContext.monitors().addMonitorListener( lastTxIdMonitor );
            final var provider = new CatchupAddressProvider.UpstreamStrategyBasedAddressProvider( topologyService, selectionStrategy );
            catchupComponents.storeCopyProcess().replaceWithStoreFrom( provider, storeId );
            databaseContext.monitors().removeMonitorListener( lastTxIdMonitor );

            debugLog.info( "Restarting local database after copy.", source );
        }
        else
        {
            ensureStoreIsPresentAt( databaseContext, catchupComponents.remoteStore(), source );
        }
    }

    private void ensureStoreIsPresentAt( ReadReplicaDatabaseContext databaseContext, RemoteStore remoteStore, ServerId upstream )
            throws StoreIdDownloadFailedException, TopologyLookupException
    {
        StoreId localStoreId = databaseContext.storeId();
        SocketAddress advertisedSocketAddress = topologyService.lookupCatchupAddress( upstream );
        StoreId remoteStoreId = remoteStore.getStoreId( advertisedSocketAddress );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException(
                    format( "This read replica cannot join the cluster. " + "The local version of %s is not empty and has a mismatching storeId: " +
                            "expected %s actual %s.", databaseContext.databaseId(), remoteStoreId, localStoreId ) );
        }
    }

    private static final class LastTxMonitor extends StoreCopyClientMonitor.Adapter
    {
        private final CommandIndexTracker commandIndexTracker;

        private LastTxMonitor( CommandIndexTracker commandIndexTracker )
        {
            this.commandIndexTracker = commandIndexTracker;
        }

        @Override
        public void finishReceivingTransactions( long endTxId )
        {
            commandIndexTracker.setAppliedCommandIndex( endTxId );
        }
    }
}
