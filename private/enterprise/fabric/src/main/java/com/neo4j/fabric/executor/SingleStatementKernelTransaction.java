/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.InputDataStreamImpl;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Rx2SyncStream;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;
import reactor.core.publisher.Flux;

import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.values.virtual.MapValue;

public class SingleStatementKernelTransaction
{
    private final ExecutingQuery parentQuery;
    private final ExecutionEngine queryExecutionEngine;
    private final TransactionalContextFactory transactionalContextFactory;
    private final InternalTransaction internalTransaction;
    private final FabricConfig config;
    private TransactionalContext executionContext;

    SingleStatementKernelTransaction( ExecutingQuery parentQuery, ExecutionEngine queryExecutionEngine, TransactionalContextFactory transactionalContextFactory,
            InternalTransaction internalTransaction, FabricConfig config )
    {
        this.parentQuery = parentQuery;
        this.queryExecutionEngine = queryExecutionEngine;
        this.transactionalContextFactory = transactionalContextFactory;
        this.internalTransaction = internalTransaction;
        this.config = config;
    }

    public StatementResult run( FullyParsedQuery query, MapValue params, Flux<Record> input )
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
            String queryText = "Internal query for Fabric query id:" + parentQuery.id();
            executionContext = transactionalContextFactory.newContext( internalTransaction, queryText, params );
            //Query is a sub-part of the parent fabric query that is already parsed and planned. The parent fabric query is monitored by the fabric executor.
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

    private InputDataStream convert( Flux<Record> input )
    {
        return new InputDataStreamImpl( new Rx2SyncStream( input, config.getDataStream().getBatchSize() )
        );
    }

    public void commit()
    {
        synchronized ( internalTransaction )
        {
            if ( internalTransaction.isOpen() )
            {
                closeContext();
                internalTransaction.commit();
            }
        }
    }

    public void rollback()
    {
        synchronized ( internalTransaction )
        {
            if ( internalTransaction.isOpen() )
            {
                closeContext();
                internalTransaction.rollback();
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

    public void terminate()
    {
        internalTransaction.terminate();
    }
}
