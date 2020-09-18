/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.replication.ReplicationResult;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.machines.lease.ClusterLeaseCoordinator;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.api.exceptions.Status.Cluster.ReplicationFailure;
import static org.neo4j.kernel.api.exceptions.Status.General.UnknownError;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LeaseExpired;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;

public class ReplicatedTransactionCommitProcess implements TransactionCommitProcess
{
    private final Replicator replicator;
    private final NamedDatabaseId namedDatabaseId;
    private final ClusterLeaseCoordinator leaseCoordinator;
    private final LogEntryWriterFactory logEntryWriterFactory;

    public ReplicatedTransactionCommitProcess( Replicator replicator, NamedDatabaseId namedDatabaseId, ClusterLeaseCoordinator leaseCoordinator,
                                               LogEntryWriterFactory logEntryWriterFactory )
    {
        this.replicator = replicator;
        this.namedDatabaseId = namedDatabaseId;
        this.leaseCoordinator = leaseCoordinator;
        this.logEntryWriterFactory = logEntryWriterFactory;
    }

    @Override
    public long commit( TransactionToApply tx, CommitEvent commitEvent, TransactionApplicationMode mode ) throws TransactionFailureException
    {
        TransactionRepresentation txRepresentation = tx.transactionRepresentation();
        int leaseId = txRepresentation.getLeaseId();

        if ( leaseId != NO_LEASE && leaseCoordinator.isInvalid( leaseId ) )
        {
            throw new TransactionFailureException( LeaseExpired, "The lease has been invalidated" );
        }

        TransactionRepresentationReplicatedTransaction transaction = ReplicatedTransaction.from( txRepresentation, namedDatabaseId, logEntryWriterFactory );

        ReplicationResult replicationResult;
        try
        {
            replicationResult = replicator.replicate( transaction );
        }
        catch ( Throwable t )
        {
            leaseCoordinator.invalidateLease( leaseId );
            throw new TransactionFailureException( ReplicationFailure, t );
        }

        switch ( replicationResult.outcome() )
        {
        case MAYBE_REPLICATED:
            leaseCoordinator.invalidateLease( leaseId );
            /* fallthrough intentional */
        case NOT_REPLICATED:
            throw new TransactionFailureException( ReplicationFailure, replicationResult.failure() );
        case APPLIED:
            try
            {
                return replicationResult.stateMachineResult().consume();
            }
            catch ( TransactionFailureException e )
            {
                throw e;
            }
            catch ( Throwable t )
            {
                // the ReplicatedTransactionStateMachine can only output TransactionFailureExceptions
                throw new TransactionFailureException( UnknownError, "Unexpected exception", t );
            }
        default:
            throw new TransactionFailureException( UnknownError, "Unexpected outcome: " + replicationResult.outcome() );
        }
    }
}
