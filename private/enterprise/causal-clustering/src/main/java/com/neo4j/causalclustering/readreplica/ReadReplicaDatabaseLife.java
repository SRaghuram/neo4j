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
import com.neo4j.causalclustering.common.ClusteredDatabaseLife;
import com.neo4j.causalclustering.core.state.snapshot.TopologyLookupException;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionException;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAbortedException;
import com.neo4j.dbms.DatabaseStartAborter;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.ExponentialBackoffStrategy;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.markUnhealthy;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.raiseAvailabilityGuard;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.stopDatabase;
import static java.lang.String.format;

class ReadReplicaDatabaseLife extends ClusteredDatabaseLife
{
    private final Lifecycle catchupProcess;
    private final Log debugLog;
    private final Log userLog;
    private final TimeoutStrategy syncRetryStrategy;
    private final UpstreamDatabaseStrategySelector selectionStrategy;
    private final SimpleStorage<RaftId> raftIdStorage;
    private final DatabaseStartAborter databaseStartAborter;
    private final ClusterInternalDbmsOperator clusterInternalOperator;
    private final TopologyService topologyService;
    private final Supplier<CatchupComponents> catchupComponentsSupplier;
    private final ReadReplicaDatabaseContext databaseContext;
    private final LifeSupport clusterComponentsLife;
    private final PanicService panicService;

    ReadReplicaDatabaseLife( ReadReplicaDatabaseContext databaseContext, Lifecycle catchupProcess, UpstreamDatabaseStrategySelector selectionStrategy,
            LogProvider debugLogProvider, LogProvider userLogProvider, TopologyService topologyService, Supplier<CatchupComponents> catchupComponentsSupplier,
            LifeSupport clusterComponentsLife, ClusterInternalDbmsOperator clusterInternalOperator, SimpleStorage<RaftId> raftIdStorage,
            PanicService panicService, DatabaseStartAborter databaseStartAborter )
    {
        this.databaseContext = databaseContext;
        this.catchupComponentsSupplier = catchupComponentsSupplier;
        this.catchupProcess = catchupProcess;
        this.selectionStrategy = selectionStrategy;
        this.panicService = panicService;
        this.raftIdStorage = raftIdStorage;
        this.databaseStartAborter = databaseStartAborter;
        this.syncRetryStrategy = new ExponentialBackoffStrategy( 1, 30, TimeUnit.SECONDS );
        this.debugLog = debugLogProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.topologyService = topologyService;
        this.clusterComponentsLife = clusterComponentsLife;
        this.clusterInternalOperator = clusterInternalOperator;
    }

    @Override
    protected void start0() throws Exception
    {
        addPanicEventHandlers();
        checkOrCreateRaftId();
        bootstrap();

        databaseContext.database().start();
        catchupProcess.start();
        topologyService.onDatabaseStart( databaseContext.databaseId() );
    }

    private void checkOrCreateRaftId() throws IOException
    {
        if ( raftIdStorage.exists() )
        {
            // If raft id state exists, read it and verify that it corresponds to the database being started
            var raftId = raftIdStorage.readState();
            var dbId = databaseContext.databaseId();
            if ( !Objects.equals( raftId.uuid(), dbId.uuid() ) )
            {
                throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                        "This may indicate a previous DROP operation for %s did not complete.", raftId.uuid(), dbId.uuid(), dbId.name() ) );
            }
        }
        else
        {
            // If the raft id state doesn't exist, create it. RaftId must correspond to the database id
            var raftId = RaftId.from( databaseContext.databaseId() );
            raftIdStorage.writeState( raftId );
        }
    }

    private void bootstrap() throws Exception
    {
        var signal = clusterInternalOperator.bootstrap( databaseContext.databaseId() );
        boolean shouldAbort = false;
        try
        {
            clusterComponentsLife.init();
            databaseContext.database().init();
            catchupProcess.init();

            clusterComponentsLife.start();
            TimeoutStrategy.Timeout syncRetryWaitPeriod = syncRetryStrategy.newTimeout();
            boolean synced = false;
            while ( !( synced || shouldAbort ) )
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
            signal.bootstrapped();
        }

        if ( shouldAbort )
        {
            throw new DatabaseStartAbortedException( format( "Database %s was stopped before it finished starting!", databaseContext.databaseId().name() ) );
        }
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
            SocketAddress fromAddress = topologyService.findCatchupAddress( source );
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
        SocketAddress advertisedSocketAddress = topologyService.findCatchupAddress( upstream );
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
        topologyService.onDatabaseStop( databaseContext.databaseId() );
        catchupProcess.stop();
        databaseContext.database().stop();
        clusterComponentsLife.stop();

        catchupProcess.shutdown();
        databaseContext.database().shutdown();
        clusterComponentsLife.stop();
        removePanicEventHandlers();
    }

    private void addPanicEventHandlers()
    {
        var db = databaseContext.database();

        var panicEventHandlers = List.of(
                raiseAvailabilityGuard( db ),
                markUnhealthy( db ),
                stopDatabase( db, clusterInternalOperator ) );

        panicService.addPanicEventHandlers( db.getDatabaseId(), panicEventHandlers );
    }

    private void removePanicEventHandlers()
    {
        panicService.removePanicEventHandlers( databaseContext.databaseId() );
    }
}
