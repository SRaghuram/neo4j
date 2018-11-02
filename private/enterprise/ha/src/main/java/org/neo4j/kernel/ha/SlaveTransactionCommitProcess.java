/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

/**
 * Commit process on slaves in HA. Transactions aren't committed here, but sent to the master, committed
 * there and streamed back. Look at {@link org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker}
 */
public class SlaveTransactionCommitProcess implements TransactionCommitProcess
{
    private final Master master;
    private final RequestContextFactory requestContextFactory;

    public SlaveTransactionCommitProcess( Master master, RequestContextFactory requestContextFactory )
    {
        this.master = master;
        this.requestContextFactory = requestContextFactory;
    }

    @Override
    public long commit( TransactionToApply batch, CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        if ( batch.next() != null )
        {
            throw new IllegalArgumentException( "Only supports single-commit on slave --> master" );
        }

        try
        {
            TransactionRepresentation representation = batch.transactionRepresentation();
            RequestContext context = requestContextFactory.newRequestContext( representation.getLockSessionId() );
            try ( Response<Long> response = master.commit( context, representation ) )
            {
                return response.response();
            }
        }
        catch ( ComException e )
        {
            throw new TransientTransactionFailureException(
                    "Cannot commit this transaction on the master. " +
                    "The master is either down, or we have network connectivity problems.", e );
        }
    }
}
