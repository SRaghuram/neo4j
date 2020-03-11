/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.executor.Location;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.StatementResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.fabric.driver.Utils.convertBookmark;

class FabricDriverAsyncTransaction implements FabricDriverTransaction
{
    private final ParameterConverter parameterConverter = new ParameterConverter();

    private final AsyncTransaction asyncTransaction;
    private final AsyncSession asyncSession;
    private final Location.Remote location;

    FabricDriverAsyncTransaction( AsyncTransaction asyncTransaction, AsyncSession asyncSession, Location.Remote location )
    {
        this.asyncTransaction = asyncTransaction;
        this.asyncSession = asyncSession;
        this.location = location;
    }

    @Override
    public Mono<RemoteBookmark> commit()
    {
        return Mono.fromFuture( asyncTransaction.commitAsync().toCompletableFuture())
                .then( Mono.fromSupplier( () -> convertBookmark( asyncSession.lastBookmark() ) ) )
                .doFinally( s -> asyncSession.closeAsync() );
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

        private final Mono<ResultCursor> statementResultCursor;
        private final RecordConverter recordConverter;

        StatementResultImpl( Mono<ResultCursor> statementResultCursor, long sourceTag )
        {
            super( statementResultCursor.map( ResultCursor::keys ).flatMapMany( Flux::fromIterable ),
                    statementResultCursor.map( ResultCursor::consumeAsync ).flatMap( Mono::fromCompletionStage ), sourceTag );
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
