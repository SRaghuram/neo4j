/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

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
        boolean syncedWithUpstream = false;
        TimeoutStrategy.Timeout syncRetryWaitPeriod = syncRetryStrategy.newTimeout();
        int attempt = 0;
        while ( !syncedWithUpstream )
        {
            attempt++;
            MemberId source;
            try
            {
                source = selectionStrategy.bestUpstreamDatabase();
                MemberId thisSource = source;
                int thisAttempt = attempt;
                CompletableFuture<Boolean> sync = syncStoresWithUpstream( source ).handle( ( ignored, errors ) -> {
                    if ( errors != null )
                    {
                        //TODO: Consider what happens when there are several exceptions thrown
                        logOrRethrowSyncException( errors, thisSource, thisAttempt );
                        return false;
                    }
                    return true;
                } );
                syncedWithUpstream = sync.join();
            }
            catch ( UpstreamDatabaseSelectionException e )
            {
                lastIssue = issueOf( "finding upstream member", attempt );
                debugLog.warn( lastIssue );
            }

            try
            {
                Thread.sleep( syncRetryWaitPeriod.getMillis() );
                syncRetryWaitPeriod.increment();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                lastIssue = "Interrupted while trying to start read replica";
                debugLog.warn( lastIssue );
                userLog.error( lastIssue );
                throw new RuntimeException( lastIssue );
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

    private void logOrRethrowSyncException( Throwable e, MemberId source, int attempt )
    {
        if ( e instanceof StoreCopyFailedException )
        {
            lastIssue = issueOf( format( "copying store files from %s", source ), attempt );
            debugLog.warn( lastIssue );
        }
        else if ( e instanceof StoreIdDownloadFailedException )
        {
            lastIssue = issueOf( format( "getting store id from %s", source ), attempt );
            debugLog.warn( lastIssue );
        }
        else if ( e instanceof TopologyLookupException )
        {
            lastIssue = issueOf( format( "getting address of %s", source ), attempt );
            debugLog.warn( lastIssue );
        }
        else if ( e instanceof CompletionException )
        {
            throw (CompletionException) e;
        }
        else
        {
            //Should be impossible
            throw new RuntimeException( e );
        }
    }

    private CompletableFuture<Void> syncStoresWithUpstream( MemberId source )
    {
        CompletableFuture[] syncRequests = databaseService.registeredDatabases().entrySet().stream().map( entry -> CompletableFuture.runAsync( () ->
        {
            try
            {
                //TODO: a few levels down this actually wraps a future which we immediately block on. Is it worth building Async methods on CatchupClients
                // to pass the future all the way up to this level and then only block at the top?
                syncStoreWithUpstream( entry.getKey(), entry.getValue(), source );
            }
            catch ( Exception e )
            {
                throw new CompletionException( e );
            }
        }, executor ) ).toArray( CompletableFuture[]::new );

        return CompletableFuture.allOf( syncRequests );
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
            AdvertisedSocketAddress fromAddress = topologyService.findCatchupAddress( source ).orElseThrow( () -> new TopologyLookupException( source ) );
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
        AdvertisedSocketAddress advertisedSocketAddress =
                topologyService.findCatchupAddress( upstream ).orElseThrow( () -> new TopologyLookupException( upstream ) );
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
}
