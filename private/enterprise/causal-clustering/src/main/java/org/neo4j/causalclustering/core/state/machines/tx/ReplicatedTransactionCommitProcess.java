/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

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
    private final String databaseName;

    public ReplicatedTransactionCommitProcess( Replicator replicator, String databaseName )
    {
        this.replicator = replicator;
        this.databaseName = databaseName;
    }

    @Override
    public long commit( final TransactionToApply tx,
                        final CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        TransactionRepresentationReplicatedTransaction transaction = ReplicatedTransaction.from( tx.transactionRepresentation(), databaseName );

        try
        {
            return (long) replicator.replicate( transaction ).consume();
        }
        catch ( ReplicationFailureException e )
        {
            throw new TransactionFailureException( ReplicationFailure, e );
        }
        catch ( TransactionFailureException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            // TODO: Panic?
            throw new RuntimeException( e );
        }
    }
}
