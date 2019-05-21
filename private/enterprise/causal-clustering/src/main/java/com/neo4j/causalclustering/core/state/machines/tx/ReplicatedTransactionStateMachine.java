/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.StateMachine;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;

import java.util.function.Consumer;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class ReplicatedTransactionStateMachine implements StateMachine<ReplicatedTransaction>
{
    private final CommandIndexTracker commandIndexTracker;
    private final ReplicatedLockTokenStateMachine lockTokenStateMachine;
    private final int maxBatchSize;
    private final Log log;
    private final PageCursorTracerSupplier pageCursorTracerSupplier;
    private final ClusteredDatabaseManager databaseManager;

    private TransactionQueue queue;
    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( CommandIndexTracker commandIndexTracker,
                                              ReplicatedLockTokenStateMachine lockStateMachine, int maxBatchSize,
                                              LogProvider logProvider,
                                              PageCursorTracerSupplier pageCursorTracerSupplier,
                                              ClusteredDatabaseManager databaseManager )
    {
        this.commandIndexTracker = commandIndexTracker;
        this.lockTokenStateMachine = lockStateMachine;
        this.maxBatchSize = maxBatchSize;
        this.log = logProvider.getLog( getClass() );
        this.pageCursorTracerSupplier = pageCursorTracerSupplier;
        this.databaseManager = databaseManager;
    }

    public synchronized void installCommitProcess( TransactionCommitProcess commitProcess, long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
        commandIndexTracker.setAppliedCommandIndex( lastCommittedIndex );
        log.info( format("Updated lastCommittedIndex to %d", lastCommittedIndex) );
        this.queue = new TransactionQueue( maxBatchSize, ( first, last ) ->
        {
            commitProcess.commit( first, CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
            pageCursorTracerSupplier.get().reportEvents(); // Report paging metrics for the commit
        } );
    }

    @Override
    public synchronized void applyCommand( ReplicatedTransaction replicatedTx, long commandIndex, Consumer<Result> callback )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.debug( "Ignoring transaction at log index %d since already committed up to %d", commandIndex, lastCommittedIndex );
            return;
        }

        TransactionRepresentation tx;

        byte[] extraHeader = encodeLogIndexAsTxHeader( commandIndex );
        tx = ReplicatedTransactionFactory.extractTransactionRepresentation( replicatedTx, extraHeader );

        int currentTokenId = lockTokenStateMachine.snapshot().candidateId();
        int txLockSessionId = tx.getLockSessionId();

        if ( currentTokenId != txLockSessionId && txLockSessionId != Locks.Client.NO_LOCK_SESSION_ID )
        {
            callback.accept( Result.of( new TransactionFailureException( LockSessionExpired,
                    "The lock session in the cluster has changed: [current lock session id:%d, tx lock session id:%d]",
                    currentTokenId, txLockSessionId ) ) );
        }
        else
        {
            try
            {
                DatabaseId databaseId = replicatedTx.databaseId();
                VersionContextSupplier versionContextSupplier = databaseManager.getDatabaseContext( databaseId ).orElseThrow(
                        () -> new DatabaseNotFoundException( databaseId.name() ) ).database().getVersionContextSupplier();
                TransactionToApply transaction = new TransactionToApply( tx, versionContextSupplier.getVersionContext() );
                transaction.onClose( txId ->
                {
                    if ( tx.getLatestCommittedTxWhenStarted() >= txId )
                    {
                        throw new IllegalStateException(
                                format( "Out of order transaction. Expected that %d < %d", tx.getLatestCommittedTxWhenStarted(), txId ) );
                    }

                    callback.accept( Result.of( txId ) );
                    commandIndexTracker.setAppliedCommandIndex( commandIndex );
                } );
                queue.queue( transaction );
            }
            catch ( Exception e )
            {
                throw panicException( e );
            }
        }
    }

    @Override
    public void flush()
    {
        // implicitly flushed
    }

    @Override
    public long lastAppliedIndex()
    {
        if ( queue == null )
        {
            /** See {@link #installCommitProcess}. */
            throw new IllegalStateException( "Value has not been installed" );
        }
        return lastCommittedIndex;
    }

    public synchronized void ensuredApplied()
    {
        try
        {
            queue.empty();
        }
        catch ( Exception e )
        {
            throw panicException( e );
        }
    }

    private IllegalStateException panicException( Exception e )
    {
        return new IllegalStateException( "Failed to locally commit a transaction that has already been " +
                "committed to the RAFT log. This server cannot process later transactions and needs to be " +
                "restarted once the underlying cause has been addressed.", e );
    }
}
