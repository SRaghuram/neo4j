/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helper.StoreValidation;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.DatabaseStartAborter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

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
    private final ClusterSystemGraphDbmsModel systemDbmsModel;

    ReadReplicaBootstrap( ReadReplicaDatabaseContext databaseContext, UpstreamDatabaseStrategySelector selectionStrategy, LogProvider debugLogProvider,
                          LogProvider userLogProvider, TopologyService topologyService, Supplier<CatchupComponents> catchupComponentsSupplier,
                          ClusterInternalDbmsOperator internalOperator, DatabaseStartAborter databaseStartAborter, TimeoutStrategy syncRetryStrategy,
                          ClusterSystemGraphDbmsModel systemDbmsModel )
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
        this.systemDbmsModel = systemDbmsModel;
    }

    public void perform() throws Exception
    {
        var bootstrapHandle = internalOperator.bootstrap( databaseContext.namedDatabaseId() );
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
                    shouldAbort = databaseStartAborter.shouldAbort( databaseContext.namedDatabaseId() );
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
            databaseStartAborter.started( databaseContext.namedDatabaseId() );
            bootstrapHandle.release();
        }

        if ( shouldAbort )
        {
            throw new DatabaseStartAbortedException( databaseContext.namedDatabaseId() );
        }
    }

    private Collection<ServerId> selectUpstreams( ReadReplicaDatabaseContext databaseContext )
    {
        try
        {
            return selectionStrategy.bestUpstreamServersForDatabase( databaseContext.namedDatabaseId() );
        }
        catch ( UpstreamDatabaseSelectionException e )
        {
            debugLog.warn( "Unable to find upstream member for " + databaseContext.namedDatabaseId() );
        }
        return List.of();
    }

    private boolean doSyncStoreCopyWithUpstream( ReadReplicaDatabaseContext databaseContext, ServerId source )
    {
        try
        {
            debugLog.info( "Syncing db: %s", databaseContext.namedDatabaseId() );
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
            debugLog.warn( "Unable to copy store files" );
            return false;
        }
        catch ( DatabaseShutdownException | IOException e )
        {
            debugLog.warn( "Syncing of stores failed unexpectedly", e );
            return false;
        }
    }

    private void syncStoreWithUpstream( ReadReplicaDatabaseContext databaseContext, ServerId sourceServer )
            throws IOException, StoreIdDownloadFailedException, StoreCopyFailedException, DatabaseShutdownException, CatchupAddressResolutionException
    {
        CatchupComponents catchupComponents = catchupComponentsSupplier.get();

        debugLog.info( "Finding store ID of upstream server %s", sourceServer );
        var sourceAddress = topologyService.lookupCatchupAddress( sourceServer );
        var remoteStoreId = catchupComponents.remoteStore().getStoreId( sourceAddress );

        if ( databaseContext.isEmpty() )
        {
            syncEmptyStore( catchupComponents, remoteStoreId );
        }
        else
        {
            syncExistingStore( databaseContext, catchupComponents, remoteStoreId );
        }
    }

    private void syncEmptyStore( CatchupComponents catchupComponents, StoreId remoteStoreId )
            throws IOException, StoreCopyFailedException, DatabaseShutdownException
    {
        debugLog.info( "Local database is empty, attempting to replace with copy from upstream servers" );
        replaceStore( remoteStoreId, catchupComponents );
    }

    private void syncExistingStore( ReadReplicaDatabaseContext databaseContext, CatchupComponents catchupComponents, StoreId remoteStoreId )
            throws IOException, StoreCopyFailedException, DatabaseShutdownException
    {
        var localAndRemoteStoreIdMatch = StoreValidation
                .validRemoteToUseTransactionsFrom( databaseContext.storeId(), remoteStoreId, databaseContext.kernelDatabase().getStorageEngineFactory() );
        var databaseIdMatch = databaseIdMatch();

        if ( localAndRemoteStoreIdMatch && databaseIdMatch )
        {
            debugLog.info( "Local store has same store id and database id as upstream. No store copy required." );
        }
        else if ( designatedSeederExists() )
        {
            debugLog.info( "Deleting local store to replace replace with copy from upstream servers. Reason is designates seeder exists, StoreId match: %s, " +
                           "DatabaseId match: %s", localAndRemoteStoreIdMatch, databaseIdMatch );
            replaceStore( remoteStoreId, catchupComponents );
        }
        else if ( !localAndRemoteStoreIdMatch )
        {
            throw new IllegalStateException( format( "This read replica cannot join the cluster. The local version of %s is not empty, it has a mismatching " +
                                                     "storeId and no designated seeder exists: expected %s actual %s.",
                                                     databaseContext.namedDatabaseId(), remoteStoreId, databaseContext.storeId() ) );
        }
        else
        {
            debugLog.info( "Local store has same store id as upstream. No store copy required." );
        }
    }

    private void replaceStore( StoreId expectedStoreId, CatchupComponents catchupComponents )
            throws IOException, StoreCopyFailedException, DatabaseShutdownException
    {
        debugLog.info( "Copying store from upstream servers" );
        databaseContext.delete();

        var provider = new CatchupAddressProvider.UpstreamStrategyBasedAddressProvider( topologyService, selectionStrategy );
        catchupComponents.storeCopyProcess().replaceWithStoreFrom( provider, expectedStoreId );

        debugLog.info( "Restarting local database after copy." );
    }

    private boolean designatedSeederExists()
    {
        var database = databaseContext.namedDatabaseId();
        return !database.isSystemDatabase() && systemDbmsModel.designatedSeeder( database ).isPresent();
    }

    private boolean databaseIdMatch()
    {
        var databaseIdReadFromSystemDatabase = databaseContext.namedDatabaseId().databaseId();
        var storeFilesDatabaseId = databaseContext.readDatabaseIdFromDisk();
        return storeFilesDatabaseId.map( databaseId -> databaseId.equals( databaseIdReadFromSystemDatabase ) )
                                   .orElse( false );
    }
}
