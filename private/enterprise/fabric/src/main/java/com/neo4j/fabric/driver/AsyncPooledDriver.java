/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
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
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.values.virtual.MapValue;

public class AsyncPooledDriver extends PooledDriver
{

    private final Driver driver;

    AsyncPooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        super( driver, releaseCallback );
        this.driver = driver;
    }

    @Override
    public AutoCommitStatementResult run( String query, MapValue params, FabricConfig.Graph location, AccessMode accessMode,
            FabricTransactionInfo transactionInfo, List<String> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode, bookmarks );
        var session = driver.asyncSession( sessionConfig );

        var parameterConverter = new ParameterConverter();
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );

        var transactionConfig = getTransactionConfig( transactionInfo );

        var statementCursor = Mono.fromFuture( session.runAsync( query, paramMap, transactionConfig ).toCompletableFuture() );
        return new StatementResultImpl( session, statementCursor, location.getId() );
    }

    @Override
    public Mono<FabricDriverTransaction> beginTransaction( FabricConfig.Graph location, AccessMode accessMode, FabricTransactionInfo transactionInfo,
            List<String> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode, bookmarks );
        var session = driver.asyncSession( sessionConfig );

        var driverTransaction = getDriverTransaction( session, transactionInfo );

        return Mono.fromFuture( driverTransaction.toCompletableFuture() ).map( tx ->  new FabricDriverAsyncTransaction( tx, session, location ));
    }

    private CompletionStage<AsyncTransaction> getDriverTransaction( AsyncSession session, FabricTransactionInfo transactionInfo )
    {
        var transactionConfig = getTransactionConfig( transactionInfo );
        return session.beginTransactionAsync( transactionConfig );
    }

    private static class StatementResultImpl extends AbstractRemoteStatementResult implements AutoCommitStatementResult
    {

        private final AsyncSession session;
        private final Mono<StatementResultCursor> statementResultCursor;
        private final RecordConverter recordConverter;
        private final CompletableFuture<String> bookmarkFuture = new CompletableFuture<>();

        StatementResultImpl( AsyncSession session, Mono<StatementResultCursor> statementResultCursor, long sourceTag )
        {
            super( statementResultCursor.map( StatementResultCursor::keys ).flatMapMany( Flux::fromIterable ),
                    statementResultCursor.map( StatementResultCursor::consumeAsync ).flatMap( Mono::fromCompletionStage ),
                    sourceTag, session::closeAsync );
            this.session = session;
            this.statementResultCursor = statementResultCursor;
            this.recordConverter = new RecordConverter( sourceTag );
        }

        @Override
        protected Flux<Record> doGetRecords()
        {
            return statementResultCursor.flatMapMany( cursor -> Flux.from( new RecordPublisher( cursor, recordConverter ) ) )
                    .doOnComplete( () -> bookmarkFuture.complete( DriverBookmarkFormat.serialize( session.lastBookmark() ) ) );
        }

        @Override
        public Mono<String> getBookmark()
        {
            return Mono.fromFuture( bookmarkFuture );
        }
    }
}
