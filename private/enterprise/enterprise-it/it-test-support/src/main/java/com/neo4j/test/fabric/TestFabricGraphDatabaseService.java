/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.fabric;

import com.neo4j.fabric.transaction.FabricTransactionImpl;
import com.neo4j.fabric.transaction.TransactionManager;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter;
import org.neo4j.values.virtual.MapValue;

import static java.util.Objects.requireNonNull;

public class TestFabricGraphDatabaseService extends GraphDatabaseFacade
{
    private static final AtomicLong TRANSACTION_COUNTER = new AtomicLong();
    private static final String TAG_NAME = "fabric-tx-id";

    final BoltGraphDatabaseServiceSPI boltFabricDatabaseService;
    final Config config;

    public TestFabricGraphDatabaseService( GraphDatabaseFacade baseDb,
                                           BoltGraphDatabaseServiceSPI boltFabricDatabaseService,
                                           Config config )
    {
        super( baseDb, Function.identity() );
        this.boltFabricDatabaseService = boltFabricDatabaseService;
        this.config = requireNonNull( config );
    }

    @Override
    protected InternalTransaction beginTransactionInternal( KernelTransaction.Type type,
                                                            LoginContext loginContext,
                                                            ClientConnectionInfo connectionInfo,
                                                            long timeoutMillis )
    {

        var fabricTxId = TRANSACTION_COUNTER.incrementAndGet();
        var boltTransaction = boltFabricDatabaseService.beginTransaction( type, loginContext, connectionInfo, List.of(),
                Duration.ofMillis( timeoutMillis ), AccessMode.WRITE, Map.of( TAG_NAME, fabricTxId) );
        forceKernelTxCreation( boltTransaction );
        var internalTransaction = findInternalTransaction( fabricTxId );
        return new TestFabricTransaction( contextFactory, boltTransaction, internalTransaction );
    }

    private void forceKernelTxCreation( BoltTransaction boltTransaction )
    {
        var future = new CompletableFuture<>();

        try
        {
            var queryExecution = boltTransaction.executeQuery( "RETURN 1", MapValue.EMPTY, true, new QuerySubscriberAdapter()
            {
                @Override
                public void onError( Throwable throwable )
                {
                    future.completeExceptionally( throwable );
                }

                @Override
                public void onResultCompleted( QueryStatistics statistics )
                {
                    future.complete( null );
                }
            } );

            queryExecution.getQueryExecution().request( Long.MAX_VALUE );
            future.get();
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Failed to run the initialization query" );
        }
    }

    private InternalTransaction findInternalTransaction( long fabricTxId )
    {
        var fabricTransaction = findFabricTransaction( fabricTxId );
        var internalTransactions = fabricTransaction.getInternalTransactions();
        assertOneTransaction( internalTransactions );
        return internalTransactions.stream().findAny().get();
    }

    private FabricTransactionImpl findFabricTransaction( long fabricTxId )
    {
        var transactionManager = getDependencyResolver().resolveDependency( TransactionManager.class );
        var foundTransactions = transactionManager.getOpenTransactions().stream()
                .filter( fabricTransaction -> isWantedTransaction( fabricTransaction.getTransactionInfo().getTxMetadata(), fabricTxId ) )
                .collect( Collectors.toList() );
        assertOneTransaction( foundTransactions );
        return foundTransactions.get( 0 );
    }

    private boolean isWantedTransaction( Map<String,Object> txMetadata, long wantedTxId )
    {
        var txId = txMetadata.get( TAG_NAME );
        return txId != null && txId.equals( wantedTxId );
    }

    private void assertOneTransaction( Collection<?> transactions )
    {
        if ( transactions.isEmpty() )
        {
            throw new IllegalStateException( "No transaction found" );
        }

        if ( transactions.size() > 1 )
        {
            throw new IllegalStateException( "More than one transaction found" );
        }
    }
}
