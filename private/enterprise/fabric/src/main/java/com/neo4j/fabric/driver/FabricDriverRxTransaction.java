/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.Summary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.values.virtual.MapValue;

class FabricDriverRxTransaction implements FabricDriverTransaction
{
    private final ParameterConverter parameterConverter = new ParameterConverter();

    private final RxTransaction rxTransaction;
    private final RxSession rxSession;
    private final FabricConfig.Graph location;

    FabricDriverRxTransaction( RxTransaction rxTransaction, RxSession rxSession, FabricConfig.Graph location )
    {
        this.rxTransaction = rxTransaction;
        this.rxSession = rxSession;
        this.location = location;
    }

    public Mono<Void> commit()
    {
        return Mono.from( rxTransaction.commit() ).then().doFinally( s -> rxSession.close() );
    }

    public Mono<Void> rollback()
    {
        return Mono.from( rxTransaction.rollback() ).then().doFinally( s -> rxSession.close() );
    }

    public StatementResult run( String query, MapValue params )
    {
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );
        var rxStatementResult = rxTransaction.run( query, paramMap );
        return new StatementResultImpl( rxStatementResult, location.getId() );
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
