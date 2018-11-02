/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.causalclustering.core.replication.ReplicationFailureException;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.api.exceptions.Status.Cluster.ReplicationFailure;

public class ReplicatedTransactionCommitProcess implements TransactionCommitProcess
{
    private final Replicator replicator;

    public ReplicatedTransactionCommitProcess( Replicator replicator )
    {
        this.replicator = replicator;
    }

    @Override
    public long commit( final TransactionToApply tx,
                        final CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        TransactionRepresentationReplicatedTransaction transaction = ReplicatedTransaction.from( tx.transactionRepresentation() );
        Future<Object> futureTxId;
        try
        {
            futureTxId = replicator.replicate( transaction, true );
        }
        catch ( ReplicationFailureException e )
        {
            throw new TransactionFailureException( ReplicationFailure, e );
        }

        try
        {
            return (long) futureTxId.get();
        }
        catch ( ExecutionException e )
        {
            if ( e.getCause() instanceof TransactionFailureException )
            {
                throw (TransactionFailureException) e.getCause();
            }
            // TODO: Panic?
            throw new RuntimeException( e );
        }
        catch ( InterruptedException e )
        {
            // TODO Wait for the transaction to possibly finish within a user configurable time, before aborting.
            throw new TransactionFailureException( "Interrupted while waiting for txId", e );
        }
    }
}
