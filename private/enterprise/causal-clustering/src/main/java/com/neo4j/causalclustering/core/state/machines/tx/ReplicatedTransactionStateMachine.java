/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.StateMachine;
import com.neo4j.causalclustering.core.state.machines.StateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseStateMachine;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LeaseExpired;

public class ReplicatedTransactionStateMachine implements StateMachine<ReplicatedTransaction>
{
    private final StateMachineCommitHelper commitHelper;
    private final ReplicatedLeaseStateMachine leaseStateMachine;
    private final int maxBatchSize;
    private final Log log;
    private final LogEntryReader reader;

    private TransactionQueue queue;
    private long lastCommittedIndex = -1;

    public ReplicatedTransactionStateMachine( StateMachineCommitHelper commitHelper, ReplicatedLeaseStateMachine leaseStateMachine,
            int maxBatchSize, LogProvider logProvider, CommandReaderFactory commandReaderFactory )
    {
        this.commitHelper = commitHelper;
        this.leaseStateMachine = leaseStateMachine;
        this.maxBatchSize = maxBatchSize;
        this.reader = new VersionAwareLogEntryReader( commandReaderFactory );
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
    public synchronized void applyCommand( ReplicatedTransaction replicatedTx, long commandIndex, Consumer<StateMachineResult> callback )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.debug( "Ignoring transaction at log index %d since already committed up to %d", commandIndex, lastCommittedIndex );
            return;
        }

        byte[] extraHeader = encodeLogIndexAsTxHeader( commandIndex );
        TransactionRepresentation tx = ReplicatedTransactionFactory.extractTransactionRepresentation( replicatedTx, extraHeader, reader );

        int currentLeaseId = leaseStateMachine.snapshot().leaseId();
        int leaseId = tx.getLeaseId();

        if ( currentLeaseId != leaseId && leaseId != LeaseService.NO_LEASE )
        {
            callback.accept( StateMachineResult.of( new TransactionFailureException( LeaseExpired,
                    "The lease used for the transaction has expired: [current lease id:%d, transaction lease id:%d]", currentLeaseId, leaseId ) ) );
        }
        else
        {
            try
            {
                LongConsumer txCommittedCallback = committedTxId -> callback.accept( StateMachineResult.of( committedTxId ) );
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
            /* See installCommitProcess method. */
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
