/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.DatabaseCatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import com.neo4j.causalclustering.helper.TimeoutStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

class ReadReplicaStartupProcess implements Lifecycle
{
    private final Executor executor;
    private final Lifecycle catchupProcessManager;
    private final Log debugLog;
    private final Log userLog;
    private final TimeoutStrategy syncRetryStrategy;
    private final UpstreamDatabaseStrategySelector selectionStrategy;
    private final TopologyService topologyService;

    private String lastIssue;

    private final CatchupComponentsRepository catchupComponents;
    private final ClusteredDatabaseManager<?> clusteredDatabaseManager;

    ReadReplicaStartupProcess( Executor executor, ClusteredDatabaseManager<?> clusteredDatabaseManager,
            Lifecycle catchupProcessManager, UpstreamDatabaseStrategySelector selectionStrategyPipeline, LogProvider debugLogProvider,
            LogProvider userLogProvider, TopologyService topologyService, CatchupComponentsRepository catchupComponents )
    {
        this( executor, clusteredDatabaseManager, catchupProcessManager, selectionStrategyPipeline, debugLogProvider, userLogProvider, topologyService,
                catchupComponents, new ExponentialBackoffStrategy( 1, 30, TimeUnit.SECONDS ) );
    }

    ReadReplicaStartupProcess( Executor executor, ClusteredDatabaseManager<?> clusteredDatabaseManager,
            Lifecycle catchupProcessManager, UpstreamDatabaseStrategySelector selectionStrategy, LogProvider debugLogProvider, LogProvider userLogProvider,
            TopologyService topologyService, CatchupComponentsRepository catchupComponents, TimeoutStrategy syncRetryStrategy )
    {
        this.executor = executor;
        this.catchupComponents = catchupComponents;
        this.clusteredDatabaseManager = clusteredDatabaseManager;
        this.catchupProcessManager = catchupProcessManager;
        this.selectionStrategy = selectionStrategy;
        this.syncRetryStrategy = syncRetryStrategy;
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.topologyService = topologyService;
    }

    @Override
    public void init() throws Exception
    {
        clusteredDatabaseManager.init();
        catchupProcessManager.init();
    }

    private String issueOf( String operation, int attempt )
    {
        return format( "Failed attempt %d of %s", attempt, operation );
    }

    @Override
    public void start() throws Exception
    {
        TimeoutStrategy.Timeout syncRetryWaitPeriod = syncRetryStrategy.newTimeout();
        var dbsToSync =  new HashMap<>( clusteredDatabaseManager.registeredDatabases() );
        int attempt = 0;
        Set<DatabaseId> syncedDbs = new HashSet<>();
        while ( !syncedDbs.equals( dbsToSync.keySet() ) )
        {
            try
            {

                attempt++;
                MemberId source;
                try
                {
                    debugLog.info( "Syncing dbs: %s", Arrays.toString( dbsToSync.keySet().toArray() ) );
                    source = selectionStrategy.bestUpstreamDatabase();
                    Map<DatabaseId,AsyncResult> results = syncStoresWithUpstream( source, dbsToSync ).get();
                    Set<DatabaseId> successful = findSuccessfullySyncedDatabaseIds( results );
                    debugLog.info( "Successfully synced dbs: %s", Arrays.toString( successful.toArray() ) );
                    syncedDbs.addAll( successful );
                }
                catch ( UpstreamDatabaseSelectionException e )
                {
                    lastIssue = issueOf( "finding upstream member", attempt );
                    debugLog.warn( lastIssue );
                }
                catch ( ExecutionException e )
                {
                    debugLog.error( "Unexpected error when syncing stores", e );
                    throw new RuntimeException( e );
                }

                Thread.sleep( syncRetryWaitPeriod.getMillis() );
                syncRetryWaitPeriod.increment();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                lastIssue = "Interrupted while trying to start read replica";
                debugLog.warn( lastIssue );
                userLog.error( lastIssue );
                throw new RuntimeException( e );
            }
        }

        clusteredDatabaseManager.start();
        catchupProcessManager.start();
    }

    private CompletableFuture<Map<DatabaseId,AsyncResult>> syncStoresWithUpstream( MemberId source,
            Map<DatabaseId,? extends ClusteredDatabaseContext> dbsToSync )
    {
        CompletableFuture<Map<DatabaseId,AsyncResult>> combinedFuture = CompletableFuture.completedFuture( new HashMap<>() );
        for ( var nameDbEntry : dbsToSync.entrySet() )
        {
            DatabaseId databaseId = nameDbEntry.getKey();
            CompletableFuture<AsyncResult> stage =
                    CompletableFuture.supplyAsync( () -> doSyncStoreCopyWithUpstream( nameDbEntry.getValue(), source ), executor );
            combinedFuture = combinedFuture.thenCombineAsync( stage, ( resultsMap, currentResult ) ->
            {
                resultsMap.put( databaseId, currentResult );
                return resultsMap;
            }, executor );
        }
        return combinedFuture;
    }

    private static Set<DatabaseId> findSuccessfullySyncedDatabaseIds( Map<DatabaseId,AsyncResult> results )
    {
        return results.entrySet()
                .stream()
                .filter( entry -> entry.getValue() == AsyncResult.SUCCESS )
                .map( Map.Entry::getKey )
                .collect( toSet() );
    }

    private AsyncResult doSyncStoreCopyWithUpstream( ClusteredDatabaseContext clusteredDatabaseContext, MemberId source )
    {
        try
        {
            syncStoreWithUpstream( clusteredDatabaseContext, source );
            return AsyncResult.SUCCESS;
        }
        catch ( TopologyLookupException e )
        {
            debugLog.warn( "getting address of %s", source );
            return AsyncResult.FAIL;
        }
        catch ( StoreIdDownloadFailedException e )
        {
            debugLog.warn( "getting store id from %s", source );
            return AsyncResult.FAIL;
        }
        catch ( StoreCopyFailedException e )
        {
            debugLog.warn( "copying store files from %s", source );
            return AsyncResult.FAIL;
        }
        catch ( DatabaseShutdownException | IOException e )
        {
            debugLog.warn( format( "syncing of stores failed unexpectedly from %s", source ), e );
            return AsyncResult.FAIL;
        }
    }

    private void syncStoreWithUpstream( ClusteredDatabaseContext clusteredDatabaseContext, MemberId source ) throws IOException,
            StoreIdDownloadFailedException, StoreCopyFailedException, TopologyLookupException, DatabaseShutdownException
    {
        DatabaseCatchupComponents catchup = catchupComponents.componentsFor( clusteredDatabaseContext.databaseName() )
                .orElseThrow( () -> new IllegalStateException(
                        String.format( "No per database catchup components exist for database %s.", clusteredDatabaseContext.databaseName() ) ) );

        if ( clusteredDatabaseContext.isEmpty() )
        {
            debugLog.info( "Local database is empty, attempting to replace with copy from upstream server %s", source );

            debugLog.info( "Finding store id of upstream server %s", source );
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source );
            StoreId storeId = catchup.remoteStore().getStoreId( fromAddress );

            debugLog.info( "Copying store from upstream server %s", source );
            clusteredDatabaseContext.delete();
            catchup.storeCopyProcess().replaceWithStoreFrom( new SingleAddressProvider( fromAddress ), storeId );

            debugLog.info( "Restarting local database after copy.", source );
        }
        else
        {
            ensureStoreIsPresentAt( clusteredDatabaseContext, catchup.remoteStore(), source );
        }
    }

    private void ensureStoreIsPresentAt( ClusteredDatabaseContext clusteredDatabaseContext, RemoteStore remoteStore, MemberId upstream )
            throws StoreIdDownloadFailedException, TopologyLookupException
    {
        StoreId localStoreId = clusteredDatabaseContext.storeId();
        AdvertisedSocketAddress advertisedSocketAddress = topologyService.findCatchupAddress( upstream );
        StoreId remoteStoreId = remoteStore.getStoreId( advertisedSocketAddress );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException( format( "This read replica cannot join the cluster. " +
                    "The local version of database %s is not empty and has a mismatching storeId: " +
                    "expected %s actual %s.", clusteredDatabaseContext.databaseName(), remoteStoreId, localStoreId ) );
        }
    }

    @Override
    public void stop() throws Exception
    {
        catchupProcessManager.stop();
        clusteredDatabaseManager.stop();
    }

    @Override
    public void shutdown() throws Exception
    {
        catchupProcessManager.shutdown();
        clusteredDatabaseManager.shutdown();
    }

    private enum AsyncResult
    {
        SUCCESS,
        FAIL
    }
}
