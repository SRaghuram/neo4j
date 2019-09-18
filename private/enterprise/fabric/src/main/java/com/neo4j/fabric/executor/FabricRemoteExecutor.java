/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.planner.api.Plan.QueryTask.QueryMode;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.AccessMode;

import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionRollbackFailed;

public class FabricRemoteExecutor
{
    private final DriverPool driverPool;

    public FabricRemoteExecutor( DriverPool driverPool )
    {
        this.driverPool = driverPool;
    }

    public FabricRemoteTransaction begin( FabricTransactionInfo transactionInfo )
    {
        return new FabricRemoteTransaction( transactionInfo );
    }

    public class FabricRemoteTransaction
    {
        private final FabricTransactionInfo transactionInfo;
        private final Set<PooledDriver> usedDrivers = new HashSet<>();
        private final Map<FabricConfig.Graph,Mono<FabricDriverTransaction>> openTransactions = new HashMap<>();
        private FabricConfig.Graph writingTo;

        private FabricRemoteTransaction( FabricTransactionInfo transactionInfo )
        {
            this.transactionInfo = transactionInfo;
        }

        Mono<StatementResult> run( FabricConfig.Graph location, String query, QueryMode mode, MapValue params )
        {
            return getDriverTransaction( location, mode )
                    .map( rxTransaction -> rxTransaction.run( query, params ) );
        }

        private PooledDriver getDriver( FabricConfig.Graph location )
        {
            PooledDriver poolDriver = driverPool.getDriver( location, transactionInfo.getLoginContext().subject() );
            usedDrivers.add( poolDriver );
            return poolDriver;
        }

        private Mono<FabricDriverTransaction> getDriverTransaction( FabricConfig.Graph location, QueryMode queryMode )
        {
            if ( transactionInfo.getAccessMode() == AccessMode.READ )
            {
                return getTransaction( location, AccessMode.READ );
            }

            if ( queryMode == QueryMode.CAN_READ_ONLY )
            {
                return getTransaction( location, AccessMode.WRITE );
            }

            if ( writingTo == null || writingTo.equals( location ) )
            {
                writingTo = location;
                return getTransaction( location, AccessMode.WRITE );
            }

            throw multipleWriteError( location, writingTo );
        }

        private Mono<FabricDriverTransaction> getTransaction( FabricConfig.Graph location, AccessMode accessMode )
        {
            return openTransactions.computeIfAbsent( location, l ->
            {
                var driver = getDriver( l );
                return driver.beginTransaction( l, accessMode, transactionInfo );
            } );
        }

        public Mono<Void> commit()
        {
            List<RuntimeException> errors = new ArrayList<>();

            return Flux.concat( openTransactions.values() )
                    .flatMap( tx ->  tx.commit() .onErrorResume( t ->
                    {
                        errors.add( new FabricException( TransactionCommitFailed, "Failed to commit remote transaction", t ) );
                        return Mono.empty();
                    } ) )
                    .then()
                    .thenMany( Flux.fromIterable( errors ) )
                    .concatWith( releaseTransactionResources() )
                    .collectList()
                    .flatMap( this::handleErrors );
        }

        public Mono<Void> rollback()
        {
            List<RuntimeException> errors = new ArrayList<>();

            List<Mono<FabricDriverTransaction>> transactions = openTransactions.values().stream()
                    .map( txMono -> txMono.onErrorResume( error -> Mono.empty() ) )
                    .collect( Collectors.toList() );

            return Flux.concat( transactions )
                    .flatMap( tx -> tx.rollback().onErrorResume( t ->
                    {
                        errors.add( new FabricException( TransactionRollbackFailed, "Failed to rollback remote transaction", t ) );
                        return Mono.empty();
                    } ) )
                    .then()
                    .thenMany( Flux.fromIterable( errors ) )
                    .concatWith( releaseTransactionResources() )
                    .collectList()
                    .flatMap( this::handleErrors );
        }

        private Flux<RuntimeException> releaseTransactionResources()
        {
            usedDrivers.forEach( PooledDriver::release );
            return Flux.empty();
        }

        private Mono<Void> handleErrors( List<RuntimeException> errors )
        {
            if ( errors.isEmpty() )
            {
                return Mono.empty();
            }

            if ( errors.size() == 1 )
            {
                return Mono.error( errors.get( 0 ) );
            }

            return Mono.error( new FabricMultiException( errors ) );
        }

        private UnsupportedOperationException multipleWriteError( FabricConfig.Graph attempt, FabricConfig.Graph writingTo )
        {
            return new UnsupportedOperationException( String.format( "Multi-shard writes not allowed. Attempted write to %s, currently writing to %s",
                    attempt,
                    writingTo ) );
        }
    }
}
