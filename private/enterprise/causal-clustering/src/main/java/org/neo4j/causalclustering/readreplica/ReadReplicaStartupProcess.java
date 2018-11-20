/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import org.neo4j.causalclustering.catchup.CatchupComponentsRepository.PerDatabaseCatchupComponents;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
    private final DatabaseService databaseService;

    ReadReplicaStartupProcess( Executor executor, DatabaseService databaseService, Lifecycle catchupProcessManager,
            UpstreamDatabaseStrategySelector selectionStrategyPipeline, LogProvider debugLogProvider, LogProvider userLogProvider,
            TopologyService topologyService, CatchupComponentsRepository catchupComponents )
    {
        this( executor, databaseService, catchupProcessManager, selectionStrategyPipeline, debugLogProvider, userLogProvider, topologyService,
                catchupComponents, new ExponentialBackoffStrategy( 1, 30, TimeUnit.SECONDS ) );
    }

    ReadReplicaStartupProcess( Executor executor, DatabaseService databaseService, Lifecycle catchupProcessManager,
            UpstreamDatabaseStrategySelector selectionStrategy, LogProvider debugLogProvider, LogProvider userLogProvider,
            TopologyService topologyService, CatchupComponentsRepository catchupComponents, TimeoutStrategy syncRetryStrategy )
    {
        this.executor = executor;
        this.catchupComponents = catchupComponents;
        this.databaseService = databaseService;
        this.catchupProcessManager = catchupProcessManager;
        this.selectionStrategy = selectionStrategy;
        this.syncRetryStrategy = syncRetryStrategy;
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.topologyService = topologyService;
    }

    @Override
    public void init() throws Throwable
    {
        databaseService.init();
        catchupProcessManager.init();
    }

    private String issueOf( String operation, int attempt )
    {
        return format( "Failed attempt %d of %s", attempt, operation );
    }

    @Override
    public void start()
    {
        TimeoutStrategy.Timeout syncRetryWaitPeriod = syncRetryStrategy.newTimeout();
        Map<String,? extends LocalDatabase> dbsToSync = databaseService.registeredDatabases();
        int attempt = 0;
        while ( dbsToSync.size() > 0 )
        {
            try
            {

                attempt++;
                MemberId source;
                try
                {
                    debugLog.info( "Syncing dbs: %s", Arrays.toString( dbsToSync.keySet().toArray() ) );
                    source = selectionStrategy.bestUpstreamDatabase();
                    Map<String,AsyncResult> results = syncStoresWithUpstream( source, dbsToSync ).get();
                    List<String> successful = results.entrySet().stream().filter( isSuccessful() ).map( getDbName() ).collect( toList() );
                    debugLog.info( "Successfully synced dbs: %s", Arrays.toString( successful.toArray() ) );
                    removeDbs( dbsToSync, successful );
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

        try
        {
            databaseService.start();
            catchupProcessManager.start();
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    private void removeDbs( Map<String,? extends LocalDatabase> dbs, List<String> toRemove )
    {
        toRemove.forEach( dbs::remove );
    }

    private Function<Map.Entry<String,AsyncResult>,String> getDbName()
    {
        return Map.Entry::getKey;
    }

    private Predicate<Map.Entry<String,AsyncResult>> isSuccessful()
    {
        return entry -> entry.getValue().successful();
    }

    private CompletableFuture<Map<String,AsyncResult>> syncStoresWithUpstream( MemberId source, Map<String,? extends LocalDatabase> dbsToSync )
    {
        CompletableFuture<Map<String,AsyncResult>> combinedFuture = CompletableFuture.completedFuture( new HashMap<>() );
        for ( Map.Entry<String,? extends LocalDatabase> nameDbEntry : dbsToSync.entrySet() )
        {
            String dbName = nameDbEntry.getKey();
            CompletableFuture<AsyncResult> stage =
                    CompletableFuture.supplyAsync( () -> doSyncStoreCopyWithUpstream( dbName, nameDbEntry.getValue(), source ), executor );
            combinedFuture = combinedFuture.thenCombineAsync( stage, ( stringAsyncResultMap, asyncResult ) ->
            {
                stringAsyncResultMap.put( dbName, asyncResult );
                return stringAsyncResultMap;
            }, executor );
        }
        return combinedFuture;
    }

    private AsyncResult doSyncStoreCopyWithUpstream( String databaseName, LocalDatabase localDatabase, MemberId source )
    {
        try
        {
            syncStoreWithUpstream( databaseName, localDatabase, source );
            return AsyncResult.success();
        }
        catch ( TopologyLookupException e )
        {
            debugLog.warn( "getting address of %s", source );
            return AsyncResult.failed( e );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            debugLog.warn( "getting store id from %s", source );
            return AsyncResult.failed( e );
        }
        catch ( StoreCopyFailedException e )
        {
            debugLog.warn( "copying store files from %s", source );
            return AsyncResult.failed( e );
        }
        catch ( DatabaseShutdownException | IOException e )
        {
            debugLog.warn( format( "syncing of stores failed unexpectedly from %s", source ), e );
            return AsyncResult.failed( e );
        }
    }

    private void syncStoreWithUpstream( String databaseName, LocalDatabase localDatabase, MemberId source ) throws IOException,
            StoreIdDownloadFailedException, StoreCopyFailedException, TopologyLookupException, DatabaseShutdownException
    {
        PerDatabaseCatchupComponents catchup = catchupComponents.componentsFor( databaseName )
                .orElseThrow( () -> new IllegalStateException( String.format( "No per database catchup components exist for database %s.", databaseName ) ) );

        if ( localDatabase.isEmpty() )
        {
            debugLog.info( "Local database is empty, attempting to replace with copy from upstream server %s", source );

            debugLog.info( "Finding store id of upstream server %s", source );
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source );
            StoreId storeId = catchup.remoteStore().getStoreId( fromAddress );

            debugLog.info( "Copying store from upstream server %s", source );
            localDatabase.delete();
            catchup.storeCopyProcess().replaceWithStoreFrom( new SingleAddressProvider( fromAddress ), storeId );

            debugLog.info( "Restarting local database after copy.", source );
        }
        else
        {
            ensureStoreIsPresentAt( databaseName, localDatabase, catchup.remoteStore(), source );
        }
    }

    private void ensureStoreIsPresentAt( String databaseName, LocalDatabase localDatabase, RemoteStore remoteStore, MemberId upstream )
            throws StoreIdDownloadFailedException, TopologyLookupException
    {
        StoreId localStoreId = localDatabase.storeId();
        AdvertisedSocketAddress advertisedSocketAddress = topologyService.findCatchupAddress( upstream );
        StoreId remoteStoreId = remoteStore.getStoreId( advertisedSocketAddress );
        if ( !localStoreId.equals( remoteStoreId ) )
        {
            throw new IllegalStateException( format( "This read replica cannot join the cluster. " +
                    "The local version of database %s is not empty and has a mismatching storeId: " +
                    "expected %s actual %s.", databaseName, remoteStoreId, localStoreId ) );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        catchupProcessManager.stop();
        databaseService.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        catchupProcessManager.shutdown();
        databaseService.shutdown();
    }

    private static class AsyncResult
    {
        private final Exception e;

        AsyncResult( Exception e )
        {
            this.e = e;
        }

        boolean successful()
        {
            return e == null;
        }

        public static AsyncResult success()
        {
            return new AsyncResult( null );
        }

        public static AsyncResult failed( Exception e )
        {
            Objects.requireNonNull( e );
            return new AsyncResult( e );
        }
    }
}
