/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.replication.ReplicationFailureException;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.error_handling.Panicker;

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
    private final Panicker panicker;

    public ReplicatedTransactionCommitProcess( Replicator replicator, String databaseName, Panicker panicker )
    {
        this.replicator = replicator;
        this.databaseName = databaseName;
        this.panicker = panicker;
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
            panicker.panic( e );
            throw new RuntimeException( "Unexpected exception caused a panic.", e );
        }
    }
}
