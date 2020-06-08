/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.fabric.driver.Utils.convertBookmark;

class FabricDriverRxTransaction implements FabricDriverTransaction
{
    private final ParameterConverter parameterConverter = new ParameterConverter();
    private final AtomicReference<FabricException> primaryException = new AtomicReference<>();

    private final RxTransaction rxTransaction;
    private final RxSession rxSession;
    private final Location.Remote location;

    FabricDriverRxTransaction( RxTransaction rxTransaction, RxSession rxSession, Location.Remote location )
    {
        this.rxTransaction = rxTransaction;
        this.rxSession = rxSession;
        this.location = location;
    }

    @Override
    public Mono<RemoteBookmark> commit()
    {
        return Mono.from( rxTransaction.commit() )
                .then( Mono.fromSupplier( () -> convertBookmark( rxSession.lastBookmark() ) ) )
                .doFinally( s -> rxSession.close() );
    }

    @Override
    public Mono<Void> rollback()
    {
        return Mono.from( rxTransaction.rollback() ).then().doFinally( s -> rxSession.close() );
    }

    @Override
    public StatementResult run( String query, MapValue params )
    {
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );
        var rxStatementResult = rxTransaction.run( query, paramMap );
        return new StatementResultImpl( rxStatementResult, location.getGraphId() );
    }

    private class StatementResultImpl extends AbstractRemoteStatementResult
    {

        private final RxResult rxStatementResult;

        StatementResultImpl( RxResult rxStatementResult, long sourceTag )
        {
            super( Mono.from( rxStatementResult.keys() ).flatMapMany( Flux::fromIterable ),
                    Mono.from( rxStatementResult.consume() ),
                    sourceTag,
                    primaryException );
            this.rxStatementResult = rxStatementResult;
        }

        @Override
        protected Flux<Record> doGetRecords()
        {
            return convertRxRecords( Flux.from( rxStatementResult.records() ) );
        }
    }
}
