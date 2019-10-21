/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.InputDataStreamImpl;
import com.neo4j.fabric.stream.Rx2SyncStream;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;

import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.values.virtual.MapValue;

public class SingleStatementKernelTransaction
{
    private final ExecutionEngine queryExecutionEngine;
    private final TransactionalContextFactory transactionalContextFactory;
    private final KernelTransaction kernelTransaction;
    private final InternalTransaction internalTransaction;
    private final FabricConfig config;
    private TransactionalContext executionContext;

    SingleStatementKernelTransaction( ExecutionEngine queryExecutionEngine, TransactionalContextFactory transactionalContextFactory,
            KernelTransaction kernelTransaction, InternalTransaction internalTransaction, FabricConfig config )
    {
        this.queryExecutionEngine = queryExecutionEngine;
        this.transactionalContextFactory = transactionalContextFactory;
        this.kernelTransaction = kernelTransaction;
        this.internalTransaction = internalTransaction;
        this.config = config;
    }

    public StatementResult run( FullyParsedQuery query, MapValue params, StatementResult input )
    {
        if ( executionContext != null )
        {
            throw new IllegalStateException( "This transaction can be used to execute only one statement" );
        }

        return StatementResults.create( subscriber -> execute( query, params, convert( input ), subscriber ) );
    }

    private QueryExecution execute( FullyParsedQuery query, MapValue params, InputDataStream input, QuerySubscriber subscriber )
    {
        try
        {
            executionContext = transactionalContextFactory.newContext( internalTransaction, "", params );
            return queryExecutionEngine.executeQuery( query, params, executionContext, true, input, subscriber );
        }
        catch ( QueryExecutionKernelException e )
        {
            // all exception thrown from execution engine are wrapped in QueryExecutionKernelException,
            // let's see if there is something better hidden in it
            if ( e.getCause() == null )
            {
                throw Exceptions.transform( Status.Statement.ExecutionFailed, e );
            }
            else
            {
                throw Exceptions.transform( Status.Statement.ExecutionFailed, e.getCause() );
            }
        }
    }

    private InputDataStream convert( StatementResult input )
    {
        return new InputDataStreamImpl(
                new Rx2SyncStream(
                        input,
                        config.getDataStream().getBufferLowWatermark(),
                        config.getDataStream().getBufferSize(),
                        config.getDataStream().getSyncBatchSize() )
        );
    }

    public void commit()
    {
        if ( kernelTransaction.isOpen() )
        {
            try
            {
                closeContext();
                kernelTransaction.commit();
            }
            catch ( TransactionFailureException e )
            {
                throw new FabricException( e.status(), e );
            }
        }
    }

    public void rollback()
    {
        if ( kernelTransaction.isOpen() )
        {

            try
            {
                closeContext();
                kernelTransaction.rollback();
            }
            catch ( TransactionFailureException e )
            {
                throw new FabricException( e.status(), e );
            }
        }
    }

    private void closeContext()
    {
        if ( executionContext != null )
        {
            executionContext.close();
        }
    }

    public void markForTermination( Status reason )
    {
        kernelTransaction.markForTermination( reason );
    }
}
