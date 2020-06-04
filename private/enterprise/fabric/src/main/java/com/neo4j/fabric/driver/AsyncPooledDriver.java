/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.fabric.driver.Utils.convertBookmark;

public class AsyncPooledDriver extends PooledDriver
{

    private final Driver driver;

    AsyncPooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        super( driver, releaseCallback );
        this.driver = driver;
    }

    @Override
    public AutoCommitStatementResult run( String query, MapValue params, Location.Remote location, AccessMode accessMode,
            FabricTransactionInfo transactionInfo, List<RemoteBookmark> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode, bookmarks );
        var session = driver.asyncSession( sessionConfig );

        var parameterConverter = new ParameterConverter();
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );

        var transactionConfig = getTransactionConfig( transactionInfo );

        var resultCursor = Mono.fromFuture( session.runAsync( query, paramMap, transactionConfig ).toCompletableFuture() );
        return new StatementResultImpl( session, resultCursor, location.getGraphId() );
    }

    @Override
    public Mono<FabricDriverTransaction> beginTransaction( Location.Remote location, AccessMode accessMode, FabricTransactionInfo transactionInfo,
            List<RemoteBookmark> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode, bookmarks );
        var session = driver.asyncSession( sessionConfig );

        var driverTransaction = getDriverTransaction( session, transactionInfo );

        return Mono.fromFuture( driverTransaction.toCompletableFuture() )
                   .onErrorMap( Neo4jException.class, Utils::translateError )
                   .map( tx -> (FabricDriverTransaction) new FabricDriverAsyncTransaction( tx, session, location ) )
                   .cache();
    }

    private CompletionStage<AsyncTransaction> getDriverTransaction( AsyncSession session, FabricTransactionInfo transactionInfo )
    {
        var transactionConfig = getTransactionConfig( transactionInfo );
        return session.beginTransactionAsync( transactionConfig );
    }

    private static class StatementResultImpl extends AbstractRemoteStatementResult implements AutoCommitStatementResult
    {

        private final AsyncSession session;
        private final Mono<ResultCursor> statementResultCursor;
        private final RecordConverter recordConverter;
        private final CompletableFuture<RemoteBookmark> bookmarkFuture = new CompletableFuture<>();

        StatementResultImpl( AsyncSession session, Mono<ResultCursor> statementResultCursor, long sourceTag )
        {
            super( statementResultCursor.map( ResultCursor::keys ).flatMapMany( Flux::fromIterable ),
                    statementResultCursor.map(ResultCursor::consumeAsync ).flatMap( Mono::fromCompletionStage ),
                    sourceTag, session::closeAsync );
            this.session = session;
            this.statementResultCursor = statementResultCursor;
            this.recordConverter = new RecordConverter( sourceTag );
        }

        @Override
        protected Flux<Record> doGetRecords()
        {
            return statementResultCursor.flatMapMany( cursor -> Flux.from( new RecordPublisher( cursor, recordConverter ) ) )
                    .doOnComplete( () ->
                    {
                        var bookmark = convertBookmark( session.lastBookmark() );
                        bookmarkFuture.complete( bookmark );
                    });
        }

        @Override
        public Mono<RemoteBookmark> getBookmark()
        {
            return Mono.fromFuture( bookmarkFuture );
        }
    }
}
