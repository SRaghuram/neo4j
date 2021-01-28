/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.readreplica.tx.AsyncTxApplier;
import com.neo4j.causalclustering.readreplica.tx.BatchinTxApplierFactory;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseStrategySelector;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.dbms.ReplicatedDatabaseEventService;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngine;

public class CatchupProcessFactory
{
    private final TopologyService topologyService;
    private final CatchupClientFactory catchupClient;
    private final UpstreamDatabaseStrategySelector selectionStrategyPipeline;
    private final CommandIndexTracker commandIndexTracker;
    private final Panicker panicker;
    private final LogProvider logProvider;
    private final Config config;
    private final CatchupComponentsRepository catchupComponents;
    private final ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch databaseEventDispatch;
    private final PageCacheTracer pageCacheTracer;
    private final AsyncTxApplier asyncTxApplier;

    CatchupProcessFactory( Panicker panicker, CatchupComponentsRepository catchupComponents, TopologyService topologyService,
            CatchupClientFactory catchUpClient, UpstreamDatabaseStrategySelector selectionStrategyPipeline, CommandIndexTracker commandIndexTracker,
            LogProvider logProvider, Config config, ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch databaseEventDispatch,
            PageCacheTracer pageCacheTracer, AsyncTxApplier asyncTxApplier )
    {
        this.panicker = panicker;
        this.logProvider = logProvider;
        this.config = config;
        this.commandIndexTracker = commandIndexTracker;
        this.catchupComponents = catchupComponents;
        this.topologyService = topologyService;
        this.catchupClient = catchUpClient;
        this.selectionStrategyPipeline = selectionStrategyPipeline;
        this.databaseEventDispatch = databaseEventDispatch;
        this.pageCacheTracer = pageCacheTracer;
        this.asyncTxApplier = asyncTxApplier;
    }

    CatchupProcessComponents create( ReadReplicaDatabaseContext databaseContext )
    {
        CatchupComponentsRepository.CatchupComponents dbCatchupComponents = catchupComponents.componentsFor( databaseContext.databaseId() ).orElseThrow(
                () -> new IllegalArgumentException( String.format( "No StoreCopyProcess instance exists for database %s.", databaseContext.databaseId() ) ) );

        // TODO: We can do better than this. Core already exposes its commit process. Why not RR.
        Supplier<TransactionCommitProcess> writableCommitProcess = () -> new InternalTransactionCommitProcess(
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( TransactionAppender.class ),
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( StorageEngine.class ) );

        long applyBatchSize = ByteUnit.mebiBytes( config.get( CausalClusteringInternalSettings.read_replica_transaction_applier_batch_size ) );
        long maxQueueSize = ByteUnit.mebiBytes( config.get( CausalClusteringInternalSettings.read_replica_transaction_applier_max_queue_size ) );

        var batchingTxApplierFactory =
                new BatchinTxApplierFactory( databaseContext, commandIndexTracker, logProvider, databaseEventDispatch, pageCacheTracer, asyncTxApplier );

        CatchupPollingProcess catchupProcess =
                new CatchupPollingProcess( maxQueueSize, applyBatchSize, databaseContext, catchupClient, batchingTxApplierFactory, databaseEventDispatch,
                        dbCatchupComponents.storeCopyProcess(), logProvider, this.panicker,
                        new CatchupAddressProvider.UpstreamStrategyBasedAddressProvider( topologyService, selectionStrategyPipeline ) );

        return new CatchupProcessComponents( catchupProcess );
    }

    static class CatchupProcessComponents extends LifeSupport
    {
        private final CatchupPollingProcess catchupProcess;

        CatchupProcessComponents( CatchupPollingProcess catchupProcess )
        {
            this.catchupProcess = catchupProcess;

            add( catchupProcess );
        }

        CatchupPollingProcess catchupProcess()
        {
            return catchupProcess;
        }
    }
}
