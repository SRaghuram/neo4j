/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import com.neo4j.causalclustering.core.state.machines.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * Accepts transactions and queues them up for being applied in batches.
 */
public class BatchingTxApplier
{
    private static final String CLUSTERING_BATCHING_TRANSACTION_TAG = "clusteringBatchingTransaction";
    private final VersionContextSupplier versionContextSupplier;

    private final PullRequestMonitor monitor;
    private final CommandIndexTracker commandIndexTracker;
    private final Log log;
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch;

    private final TransactionCommitProcess commitProcess;
    private final PageCacheTracer pageCacheTracer;
    private final AsyncTxApplier applier;
    private TransactionChain transactionChain = new TransactionChain();

    private volatile long lastQueuedTxId;

    BatchingTxApplier( long lastCommittedTxId, TransactionCommitProcess commitProcess,
            Monitors monitors, VersionContextSupplier versionContextSupplier,
            CommandIndexTracker commandIndexTracker, LogProvider logProvider, ReplicatedDatabaseEventDispatch databaseEventDispatch,
            PageCacheTracer pageCacheTracer, AsyncTxApplier applier )
    {
        this.log = logProvider.getLog( getClass() );
        this.monitor = monitors.newMonitor( PullRequestMonitor.class );
        this.versionContextSupplier = versionContextSupplier;
        this.commandIndexTracker = commandIndexTracker;
        this.databaseEventDispatch = databaseEventDispatch;
        this.lastQueuedTxId = lastCommittedTxId;
        this.commitProcess = commitProcess;
        this.pageCacheTracer = pageCacheTracer;
        this.applier = applier;
    }

    /**
     * Queues a transaction for application.
     *
     * @param tx The transaction to be queued for application.
     */
    public synchronized void queue( CommittedTransactionRepresentation tx )
    {
        long receivedTxId = tx.getCommitEntry().getTxId();
        long expectedTxId = lastQueuedTxId + 1;

        if ( receivedTxId != expectedTxId )
        {
            log.warn( "Out of order transaction. Received: %d Expected: %d", receivedTxId, expectedTxId );
            return;
        }

        var cursorTracer = pageCacheTracer.createPageCursorTracer( CLUSTERING_BATCHING_TRANSACTION_TAG );

        var toApply = new TransactionToApply( tx.getTransactionRepresentation(), receivedTxId, versionContextSupplier, cursorTracer );
        toApply.onClose( txId ->
        {
            databaseEventDispatch.fireTransactionCommitted( txId );
            cursorTracer.close();
        } );
        transactionChain.chain( toApply );

        lastQueuedTxId = receivedTxId;
        monitor.txPullResponse( receivedTxId );
    }

    public synchronized void applyBatchAsync( AsyncTaskEventHandler asyncTaskEventHandler )
    {
        if ( transactionChain.first == null )
        {
            asyncTaskEventHandler.onSuccess();
            return;
        }
        var toApply = transactionChain;
        transactionChain = new TransactionChain();
        applier.add( new AsyncTask( () ->
        {
            commit( toApply );
            return null;
        }, () -> false, asyncTaskEventHandler ) );
    }

    private void commit( TransactionChain transactionChain ) throws TransactionFailureException
    {
        commitProcess.commit( transactionChain.first, NULL, EXTERNAL );
        long lastAppliedRaftLogIndex =
                LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader( transactionChain.last.transactionRepresentation().additionalHeader() );
        commandIndexTracker.setAppliedCommandIndex( lastAppliedRaftLogIndex );
    }

    /**
     * @return The id of the last transaction applied.
     */
    public long lastQueuedTxId()
    {
        return lastQueuedTxId;
    }

    private static class TransactionChain
    {
        TransactionToApply first;
        TransactionToApply last;

        private void chain( TransactionToApply next )
        {
            if ( first == null )
            {
                first = last = next;
            }
            else
            {
                last.next( next );
                last = next;
            }
        }
    }
}
