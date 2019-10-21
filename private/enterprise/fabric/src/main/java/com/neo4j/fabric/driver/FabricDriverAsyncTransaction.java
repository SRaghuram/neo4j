/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.StatementResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.values.virtual.MapValue;

class FabricDriverAsyncTransaction implements FabricDriverTransaction
{
    private final ParameterConverter parameterConverter = new ParameterConverter();

    private final AsyncTransaction asyncTransaction;
    private final AsyncSession asyncSession;
    private final FabricConfig.Graph location;

    FabricDriverAsyncTransaction( AsyncTransaction asyncTransaction, AsyncSession asyncSession, FabricConfig.Graph location )
    {
        this.asyncTransaction = asyncTransaction;
        this.asyncSession = asyncSession;
        this.location = location;
    }

    @Override
    public Mono<Void> commit()
    {
        return Mono.fromFuture( asyncTransaction.commitAsync().toCompletableFuture()).then().doFinally( s -> asyncSession.closeAsync() );
    }

    @Override
    public Mono<Void> rollback()
    {
        return Mono.fromFuture( asyncTransaction.rollbackAsync().toCompletableFuture()).then().doFinally( s -> asyncSession.closeAsync() );
    }

    @Override
    public StatementResult run( String query, MapValue params )
    {
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );
        var statementResultCursor = Mono.fromFuture( asyncTransaction.runAsync( query, paramMap ).toCompletableFuture() );
        return new StatementResultImpl( statementResultCursor, location.getId() );
    }

    private static class StatementResultImpl extends AbstractRemoteStatementResult
    {

        private final Mono<StatementResultCursor> statementResultCursor;
        private final RecordConverter recordConverter;

        StatementResultImpl( Mono<StatementResultCursor> statementResultCursor, long sourceTag )
        {
            super( statementResultCursor.map( StatementResultCursor::keys ).flatMapMany( Flux::fromIterable ),
                    statementResultCursor.map( StatementResultCursor::summaryAsync ).flatMap( Mono::fromCompletionStage ), sourceTag );
            this.statementResultCursor = statementResultCursor;
            this.recordConverter = new RecordConverter( sourceTag );
        }

        @Override
        protected Flux<Record> doGetRecords()
        {
            return statementResultCursor.flatMapMany( cursor -> Flux.from( new RecordPublisher( cursor, recordConverter ) ) );
        }
    }
}
