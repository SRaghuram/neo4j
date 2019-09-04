/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.StateMachine;
import com.neo4j.causalclustering.core.state.machines.StateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenStateMachine;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.Epoch;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class ReplicatedTransactionStateMachine implements StateMachine<ReplicatedTransaction>
{
    private final StateMachineCommitHelper commitHelper;
    private final ReplicatedBarrierTokenStateMachine lockTokenStateMachine;
    private final int maxBatchSize;
    private final Log log;
    private final LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();

    private TransactionQueue queue;
    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( StateMachineCommitHelper commitHelper, ReplicatedBarrierTokenStateMachine lockStateMachine,
            int maxBatchSize, LogProvider logProvider )
    {
        this.commitHelper = commitHelper;
        this.lockTokenStateMachine = lockStateMachine;
        this.maxBatchSize = maxBatchSize;
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized void installCommitProcess( TransactionCommitProcess commitProcess, long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
        this.commitHelper.updateLastAppliedCommandIndex( lastCommittedIndex );
        log.info( format("Updated lastCommittedIndex to %d", lastCommittedIndex) );
        this.queue = new TransactionQueue( maxBatchSize, ( first, last ) -> commitHelper.commit( commitProcess, first ) );
    }

    @Override
    public synchronized void applyCommand( ReplicatedTransaction replicatedTx, long commandIndex, Consumer<Result> callback )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.debug( "Ignoring transaction at log index %d since already committed up to %d", commandIndex, lastCommittedIndex );
            return;
        }

        byte[] extraHeader = encodeLogIndexAsTxHeader( commandIndex );
        TransactionRepresentation tx = ReplicatedTransactionFactory.extractTransactionRepresentation( replicatedTx, extraHeader, reader );

        int currentTokenId = lockTokenStateMachine.snapshot().candidateId();
        int txTokenId = tx.getEpochTokenId();

        if ( currentTokenId != txTokenId && txTokenId != Epoch.NO_EPOCH )
        {
            callback.accept( Result.of( new TransactionFailureException( LockSessionExpired,
                    "The epoch token in the cluster has changed: [current token id:%d, tx token id:%d]",
                    currentTokenId, txTokenId ) ) );
        }
        else
        {
            try
            {
                LongConsumer txCommittedCallback = committedTxId -> callback.accept( Result.of( committedTxId ) );
                TransactionToApply transaction = commitHelper.newTransactionToApply( tx, commandIndex, txCommittedCallback );
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
