/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.auth.CredentialsProvider;
import com.neo4j.fabric.driver.RecordConverter;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.fabric.driver.ParameterConverter;
import com.neo4j.fabric.planner.api.Plan.QueryTask.QueryMode;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.Summary;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.AccessMode;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Driver;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionRollbackFailed;

public class FabricRemoteExecutor
{
    private final ParameterConverter parameterConverter = new ParameterConverter();
    private final DriverPool driverPool;
    private final CredentialsProvider credentialsProvider;

    public FabricRemoteExecutor( DriverPool driverPool, CredentialsProvider credentialsProvider )
    {
        this.driverPool = driverPool;
        this.credentialsProvider = credentialsProvider;
    }

    public FabricRemoteTransaction begin( FabricTransactionInfo transactionInfo )
    {
        return new FabricRemoteTransaction( transactionInfo );
    }

    public class FabricRemoteTransaction
    {
        private final FabricTransactionInfo transactionInfo;
        private final Set<PooledDriver> usedDrivers = new HashSet<>();
        private final Map<FabricConfig.Graph,Mono<RxTransaction>> openTransactions = new HashMap<>();
        private final Set<RxSession> usedSessions = new HashSet<>();
        private FabricConfig.Graph writingTo;

        private FabricRemoteTransaction( FabricTransactionInfo transactionInfo )
        {
            this.transactionInfo = transactionInfo;
        }

        Mono<StatementResult> run( FabricConfig.Graph location, String query, QueryMode mode, MapValue params )
        {
            Map<String,Object> paramMap = (Map<String,Object>) parameterConverter.convertValue( params );
            return getDriverTransaction( location, mode )
                    .map( rxTransaction -> rxTransaction.run( query, paramMap ) )
                    .map( rxStatementResult -> new StatementResultImpl( rxStatementResult, location.getId() ) );
        }

        private Driver getDriver( FabricConfig.Graph location )
        {
            AuthToken authToken = credentialsProvider.credentialsFor( transactionInfo.getLoginContext().subject() );
            PooledDriver poolDriver = driverPool.getDriver( location, authToken );
            usedDrivers.add( poolDriver );
            return poolDriver.getDriver();
        }

        private Mono<RxTransaction> getDriverTransaction( FabricConfig.Graph location, QueryMode queryMode )
        {
            if ( transactionInfo.getAccessMode() == AccessMode.READ )
            {
                return getTransaction( location, org.neo4j.driver.AccessMode.READ );
            }

            if ( queryMode == QueryMode.CAN_READ_ONLY )
            {
                return getTransaction( location, org.neo4j.driver.AccessMode.WRITE );
            }

            if ( writingTo == null || writingTo.equals( location ) )
            {
                writingTo = location;
                return getTransaction( location, org.neo4j.driver.AccessMode.WRITE );
            }

            throw multipleWriteError( location, writingTo );
        }

        private Mono<RxTransaction> getTransaction( FabricConfig.Graph location, org.neo4j.driver.AccessMode accessMode )
        {
            return openTransactions.computeIfAbsent( location, l ->
            {
                Driver driver = getDriver( l );
                RxSession session =
                        driver.rxSession( SessionConfig.builder().withDefaultAccessMode( accessMode ).withDatabase( location.getDatabase() ).build() );
                usedSessions.add( session );

                if ( transactionInfo.getTxTimeout().equals( Duration.ZERO ) )
                {

                    return Mono.from( session.beginTransaction() ).cache();
                }

                return Mono.from( session.beginTransaction( TransactionConfig.builder().withTimeout( transactionInfo.getTxTimeout() ).build() ) ).cache();
            } );
        }

        public Mono<Void> commit()
        {
            List<RuntimeException> errors = new ArrayList<>();

            return Flux.concat( openTransactions.values() )
                    .flatMap( tx -> Mono.from( tx.commit() ).onErrorResume( t ->
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

            List<Mono<RxTransaction>> transactions = openTransactions.values().stream()
                    .map( txMono -> txMono.onErrorResume( error -> Mono.empty() ) )
                    .collect( Collectors.toList() );

            return Flux.concat( transactions )
                    .flatMap( tx -> Mono.from( tx.rollback() ).onErrorResume( t ->
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
            List<RuntimeException> errors = new ArrayList<>();

            return Flux.fromIterable( usedSessions )
                    .flatMap( session -> Mono.from( session.close() ).onErrorResume( t ->
                    {
                        errors.add( new FabricException( TransactionRollbackFailed, "Failed to close remote session", t ) );
                        return Mono.empty();
                    } )
                            .then()
                            .doOnNext( nothing -> usedDrivers.forEach( PooledDriver::release ) )
                            .thenMany( Flux.fromIterable( errors ) ) );
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

    private static class StatementResultImpl implements StatementResult
    {

        private final RxStatementResult rxStatementResult;
        private final RecordConverter recordConverter;

        StatementResultImpl( RxStatementResult rxStatementResult, long sourceTag )
        {
            this.rxStatementResult = rxStatementResult;
            recordConverter = new RecordConverter( sourceTag );
        }

        @Override
        public Flux<String> columns()
        {
            return Flux.from( rxStatementResult.keys() );
        }

        @Override
        public Flux<Record> records()
        {
            return Flux.from( rxStatementResult.records() ).map( driverRecord -> Records.lazy( driverRecord.size(),
                    () -> Records.of( driverRecord.values().stream()
                            .map( recordConverter::convertValue )
                            .collect( Collectors.toList() ) ) ) );
        }

        @Override
        public Mono<Summary> summary()
        {
            return Mono.empty();
        }
    }
}
