/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.summary.Summary;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.values.virtual.MapValue;

class RxPooledDriver extends PooledDriver
{

    private final Driver driver;

    RxPooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        super( driver, releaseCallback );
        this.driver = driver;
    }

    @Override
    public AutoCommitStatementResult run( String query, MapValue params, FabricConfig.Graph location, AccessMode accessMode,
            FabricTransactionInfo transactionInfo, List<String> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode );
        var session = driver.rxSession( sessionConfig );

        var parameterConverter = new ParameterConverter();
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );

        var transactionConfig = getTransactionConfig( transactionInfo );
        var rxStatementResult = session.run( query, paramMap, transactionConfig );

        return new StatementResultImpl( session, rxStatementResult, location.getId() );
    }

    @Override
    public Mono<FabricDriverTransaction> beginTransaction( FabricConfig.Graph location, AccessMode accessMode, FabricTransactionInfo transactionInfo,
            List<String> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode );
        var session = driver.rxSession( sessionConfig );

        var driverTransaction = getDriverTransaction( session, transactionInfo );

        return driverTransaction.map( tx ->  new FabricDriverRxTransaction( tx, session, location ));
    }

    private Mono<RxTransaction> getDriverTransaction( RxSession session, FabricTransactionInfo transactionInfo )
    {
        var transactionConfig = getTransactionConfig( transactionInfo );
        return Mono.from( session.beginTransaction( transactionConfig ) ).cache();
    }

    private static class StatementResultImpl implements AutoCommitStatementResult
    {

        private final RxSession session;
        private final RxStatementResult rxStatementResult;
        private final RecordConverter recordConverter;

        StatementResultImpl( RxSession session, RxStatementResult rxStatementResult, long sourceTag )
        {
            this.session = session;
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
            return Flux.from( rxStatementResult.records() )
                    .map( driverRecord -> Records.lazy( driverRecord.size(),
                    () -> Records.of( driverRecord.values().stream()
                            .map( recordConverter::convertValue )
                            .collect( Collectors.toList() ) ) ) )
                    .doFinally( signalType -> session.close() );
        }

        @Override
        public Mono<Summary> summary()
        {
            return Mono.empty();
        }

        @Override
        public String getBookmark()
        {
            return session.lastBookmark();
        }
    }
}
