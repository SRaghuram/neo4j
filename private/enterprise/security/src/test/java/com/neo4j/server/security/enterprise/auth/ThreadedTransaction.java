/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class ThreadedTransaction<S>
{
    private volatile Future<Throwable> done;
    private final NeoInteractionLevel<S> neo;
    private final DoubleLatch latch;

    ThreadedTransaction( NeoInteractionLevel<S> neo, DoubleLatch latch  )
    {
        this.neo = neo;
        this.latch = latch;
    }

    String executeCreateNode( ThreadingRule threading, S subject )
    {
        final String query = "CREATE (:Test { name: '" + neo.nameOf( subject ) + "-node'})";
        return execute( threading, subject, query );
    }

    String execute( ThreadingRule threading, S subject, String query )
    {
        return doExecute( threading, subject, KernelTransaction.Type.EXPLICIT, false, query )[0];
    }

    String execute( ThreadingRule threading, String database, S subject, String query )
    {
        return doExecute( threading, subject, KernelTransaction.Type.EXPLICIT, database, false, query )[0];
    }

    String[] execute( ThreadingRule threading, S subject, String... queries )
    {
        return doExecute( threading, subject, KernelTransaction.Type.EXPLICIT, false, queries );
    }

    String executeEarly( ThreadingRule threading, S subject, KernelTransaction.Type txType, String query )
    {
        return doExecute( threading, subject, txType, true, query )[0];
    }

    private String[] doExecute(
            ThreadingRule threading, S subject, KernelTransaction.Type txType, boolean startEarly, String... queries )
    {
        return doExecute( threading, subject, txType, DEFAULT_DATABASE_NAME, startEarly, queries );
    }

    private String[] doExecute(
        ThreadingRule threading, S subject, KernelTransaction.Type txType, String database, boolean startEarly, String... queries )
    {
        NamedFunction<S, Throwable> startTransaction =
                new NamedFunction<>( "threaded-transaction-" + Arrays.hashCode( queries ) )
                {
                    @Override
                    public Throwable apply( S subject )
                    {
                        try ( InternalTransaction tx = neo.beginLocalTransactionAsUser( subject, txType, database ) )
                        {
                            Result result = null;
                            try
                            {
                                if ( startEarly )
                                {
                                    latch.start();
                                }
                                for ( String query : queries )
                                {
                                    if ( result != null )
                                    {
                                        result.accept( row -> true );
                                        result.close();
                                    }
                                    result = tx.execute( query );
                                }
                                if ( !startEarly )
                                {
                                    latch.startAndWaitForAllToStart();
                                }
                            }
                            finally
                            {
                                if ( !startEarly )
                                {
                                    latch.start();
                                }
                                latch.finishAndWaitForAllToFinish();
                            }
                            if ( result != null )
                            {
                                result.accept( row -> true );
                                result.close();
                            }
                            tx.commit();
                            return null;
                        }
                        catch ( Throwable t )
                        {
                            return t;
                        }
                    }
                };

        done = threading.execute( startTransaction, subject );
        return queries;
    }

    void closeAndAssertSuccess() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread != null )
        {
            throw new AssertionError( "Expected no exception in ThreadCreate, but got one.", exceptionInOtherThread );
        }
    }

    void closeAndAssertExplicitTermination() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread == null )
        {
            fail( "Expected explicit TransactionTerminatedException in the threaded transaction, " +
                    "but no exception was raised" );
        }
        assertThat( exceptionInOtherThread.getMessage() ).as( Exceptions.stringify( exceptionInOtherThread ) ).contains( "Explicitly terminated by the user." );
    }

    void closeAndAssertSomeTermination() throws Throwable
    {
        Throwable exceptionInOtherThread = join();
        if ( exceptionInOtherThread == null )
        {
            fail( "Expected a TransactionTerminatedException in the threaded transaction, but no exception was raised" );
        }
        assertThat( exceptionInOtherThread ).as( Exceptions.stringify( exceptionInOtherThread ) ).isInstanceOf( TransactionTerminatedException.class );
    }

    private Throwable join() throws ExecutionException, InterruptedException
    {
        return done.get();
    }
}
