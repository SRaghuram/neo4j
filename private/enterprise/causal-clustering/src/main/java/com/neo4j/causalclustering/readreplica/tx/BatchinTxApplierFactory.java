/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.readreplica.ReadReplicaDatabaseContext;
import com.neo4j.dbms.ReplicatedDatabaseEventService;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionIdStore;

public class BatchinTxApplierFactory
{
    private final ReadReplicaDatabaseContext databaseContext;
    private final CommandIndexTracker commandIndexTracker;
    private final LogProvider logProvider;
    private final ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch databaseEventDispatch;
    private final PageCacheTracer pageCacheTracer;
    private final AsyncTxApplier asyncTxApplier;

    public BatchinTxApplierFactory( ReadReplicaDatabaseContext databaseContext, CommandIndexTracker commandIndexTracker,
            LogProvider logProvider, ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch databaseEventDispatch,
            PageCacheTracer pageCacheTracer, AsyncTxApplier asyncTxApplier )
    {
        this.databaseContext = databaseContext;
        this.commandIndexTracker = commandIndexTracker;
        this.logProvider = logProvider;
        this.databaseEventDispatch = databaseEventDispatch;
        this.pageCacheTracer = pageCacheTracer;
        this.asyncTxApplier = asyncTxApplier;
    }

    public BatchingTxApplier create()
    {
        var internalTransactionCommitProcess = new InternalTransactionCommitProcess(
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( TransactionAppender.class ),
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( StorageEngine.class ) );

        var lastCommittedTransactionId =
                databaseContext.kernelDatabase().getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();

        return new BatchingTxApplier( lastCommittedTransactionId, internalTransactionCommitProcess, databaseContext.monitors(),
                databaseContext.kernelDatabase().getVersionContextSupplier(), commandIndexTracker, logProvider,
                databaseEventDispatch, pageCacheTracer, asyncTxApplier );
    }
}
