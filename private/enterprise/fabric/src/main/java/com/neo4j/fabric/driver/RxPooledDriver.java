/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.fabric.executor.ExecutionOptions;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.values.virtual.MapValue;

import static com.neo4j.fabric.driver.Utils.convertBookmark;

class RxPooledDriver extends PooledDriver
{

    private final Driver driver;

    RxPooledDriver( Driver driver, Consumer<PooledDriver> releaseCallback )
    {
        super( driver, releaseCallback );
        this.driver = driver;
    }

    @Override
    public AutoCommitStatementResult run( String query, MapValue params, Location.Remote location, ExecutionOptions options, AccessMode accessMode,
            FabricTransactionInfo transactionInfo, List<RemoteBookmark> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode, bookmarks );
        var session = driver.rxSession( sessionConfig );

        var parameterConverter = new ParameterConverter();
        var paramMap = (Map<String,Object>) parameterConverter.convertValue( params );

        var transactionConfig = getTransactionConfig( transactionInfo );
        var rxResult = session.run( query, paramMap, transactionConfig );

        return new StatementResultImpl( session, rxResult, options );
    }

    @Override
    public Mono<FabricDriverTransaction> beginTransaction( Location.Remote location, ExecutionOptions options, AccessMode accessMode,
            FabricTransactionInfo transactionInfo, List<RemoteBookmark> bookmarks )
    {
        var sessionConfig = createSessionConfig( location, accessMode, bookmarks );
        var session = driver.rxSession( sessionConfig );

        var driverTransaction = getDriverTransaction( session, transactionInfo );

        return driverTransaction
                .onErrorMap( Neo4jException.class, Utils::translateError )
                .map( tx -> (FabricDriverTransaction) new FabricDriverRxTransaction( tx, session, options ) )
                .cache();
    }

    private Mono<RxTransaction> getDriverTransaction( RxSession session, FabricTransactionInfo transactionInfo )
    {
        var transactionConfig = getTransactionConfig( transactionInfo );
        return Mono.from( session.beginTransaction( transactionConfig ) );
    }

    private static class StatementResultImpl extends AbstractRemoteStatementResult implements AutoCommitStatementResult
    {

        private final RxSession session;
        private final RxResult rxResult;
        private final CompletableFuture<RemoteBookmark> bookmarkFuture = new CompletableFuture<>();

        StatementResultImpl( RxSession session, RxResult rxResult, ExecutionOptions options )
        {
            super( Mono.from( rxResult.keys() ).flatMapMany( Flux::fromIterable ),
                    Mono.from( rxResult.consume() ),
                    options, session::close
            );
            this.session = session;
            this.rxResult = rxResult;
        }

        @Override
        public  Mono<RemoteBookmark> getBookmark()
        {
            return Mono.fromFuture( bookmarkFuture );
        }

        @Override
        protected Flux<Record> doGetRecords()
        {
            return convertRxRecords( Flux.from( rxResult.records() ) )
                    .doOnComplete( () ->
                    {
                        var bookmark = convertBookmark( session.lastBookmark() );
                        bookmarkFuture.complete( bookmark );
                    });
        }
    }
}
