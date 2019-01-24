/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;

import java.util.function.Supplier;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * Accepts transactions and queues them up for being applied in batches.
 */
public class BatchingTxApplier extends LifecycleAdapter
{
    private final int maxBatchSize;
    private final Supplier<TransactionIdStore> txIdStoreSupplier;
    private final Supplier<TransactionCommitProcess> commitProcessSupplier;

    private final PullRequestMonitor monitor;
    private final PageCursorTracerSupplier pageCursorTracerSupplier;
    private final VersionContextSupplier versionContextSupplier;
    private final CommandIndexTracker commandIndexTracker;
    private final Log log;

    private TransactionQueue txQueue;
    private TransactionCommitProcess commitProcess;

    private volatile long lastQueuedTxId;
    private volatile boolean stopped;

    public BatchingTxApplier( int maxBatchSize, Supplier<TransactionIdStore> txIdStoreSupplier,
                              Supplier<TransactionCommitProcess> commitProcessSupplier, Monitors monitors,
                              PageCursorTracerSupplier pageCursorTracerSupplier,
                              VersionContextSupplier versionContextSupplier, CommandIndexTracker commandIndexTracker,
                              LogProvider logProvider )
    {
        this.maxBatchSize = maxBatchSize;
        this.txIdStoreSupplier = txIdStoreSupplier;
        this.commitProcessSupplier = commitProcessSupplier;
        this.pageCursorTracerSupplier = pageCursorTracerSupplier;
        this.log = logProvider.getLog( getClass() );
        this.monitor = monitors.newMonitor( PullRequestMonitor.class );
        this.versionContextSupplier = versionContextSupplier;
        this.commandIndexTracker = commandIndexTracker;
    }

    @Override
    public void start()
    {
        stopped = false;
        refreshFromNewStore();
        txQueue = new TransactionQueue( maxBatchSize, ( first, last ) ->
        {
            commitProcess.commit( first, NULL, EXTERNAL );
            pageCursorTracerSupplier.get().reportEvents();  // Report paging metrics for the commit
            long lastAppliedRaftLogIndex = LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader( last.transactionRepresentation().additionalHeader() );
            commandIndexTracker.setAppliedCommandIndex( lastAppliedRaftLogIndex );
        } );
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    void refreshFromNewStore()
    {
        assert txQueue == null || txQueue.isEmpty();
        lastQueuedTxId = txIdStoreSupplier.get().getLastCommittedTransactionId();
        commitProcess = commitProcessSupplier.get();
    }

    /**
     * Queues a transaction for application.
     *
     * @param tx The transaction to be queued for application.
     */
    public void queue( CommittedTransactionRepresentation tx ) throws Exception
    {
        long receivedTxId = tx.getCommitEntry().getTxId();
        long expectedTxId = lastQueuedTxId + 1;

        if ( receivedTxId != expectedTxId )
        {
            log.warn( "Out of order transaction. Received: %d Expected: %d", receivedTxId, expectedTxId );
            return;
        }

        txQueue.queue( new TransactionToApply( tx.getTransactionRepresentation(), receivedTxId, versionContextSupplier.getVersionContext() ) );

        if ( !stopped )
        {
            lastQueuedTxId = receivedTxId;
            monitor.txPullResponse( receivedTxId );
        }
    }

    void applyBatch() throws Exception
    {
        txQueue.empty();
    }

    /**
     * @return The id of the last transaction applied.
     */
    long lastQueuedTxId()
    {
        return lastQueuedTxId;
    }
}
