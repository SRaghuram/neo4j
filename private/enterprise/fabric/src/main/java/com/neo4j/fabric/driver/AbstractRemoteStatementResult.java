/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.FabricSecondaryException;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Records;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.api.exceptions.Status;

abstract class AbstractRemoteStatementResult implements StatementResult
{

    private final Flux<String> columns;
    private final Mono<ResultSummary> summary;
    private final Mono<QueryExecutionType> executionType;
    private final RecordConverter recordConverter;
    private final Runnable completionListener;
    private final AtomicReference<FabricException> primaryException;
    private boolean completionInvoked;

    AbstractRemoteStatementResult( Flux<String> columns, Mono<ResultSummary> summary, long sourceTag, AtomicReference<FabricException> primaryException )
    {
        this( columns, summary, sourceTag, primaryException, () ->
        {
        } );
    }

    AbstractRemoteStatementResult( Flux<String> columns, Mono<ResultSummary> summary, long sourceTag, Runnable completionListener )
    {
        this( columns, summary, sourceTag, new AtomicReference<>(), completionListener );
    }

    private AbstractRemoteStatementResult( Flux<String> columns, Mono<ResultSummary> summary, long sourceTag, AtomicReference<FabricException> primaryException,
            Runnable completionListener )
    {
        this.columns = columns;
        this.summary = summary;
        // TODO: This is totally wrong, but real value is required before summary is available.
        // Either we do analysis in fabric and send in here, or we make sure value is not needed until after execution
        this.executionType = Mono.just( QueryExecutionType.query( QueryExecutionType.QueryType.READ_WRITE ) );
        recordConverter = new RecordConverter( sourceTag );
        this.completionListener = completionListener;
        this.primaryException = primaryException;
    }

    @Override
    public Flux<String> columns()
    {
        return columns.onErrorMap( Neo4jException.class, this::handleError ).doOnError( ignored -> invokeCompletionListener() );
    }

    @Override
    public Flux<Record> records()
    {
        return doGetRecords()
                .onErrorMap( Neo4jException.class, this::handleError )
                .doFinally( signalType -> invokeCompletionListener() );
    }

    @Override
    public Mono<Summary> summary()
    {
        return summary.onErrorMap( Neo4jException.class, this::handleError )
            .map( ResultSummaryWrapper::new );
    }

    @Override
    public Mono<QueryExecutionType> executionType()
    {
        return executionType;
    }

    protected abstract Flux<Record> doGetRecords();

    protected Flux<Record> convertRxRecords( Flux<org.neo4j.driver.Record> records )
    {
        return records.map( driverRecord -> Records.lazy( driverRecord.size(), () -> Records.of( driverRecord.values().stream()
                .map( recordConverter::convertValue )
                .collect( Collectors.toList() ) ) ) );
    }

    private void invokeCompletionListener()
    {
        if ( !completionInvoked )
        {
            completionListener.run();
            completionInvoked = true;
        }
    }

    private FabricException handleError( Neo4jException driverException )
    {
        FabricException translatedException = translateError( driverException );

        if ( primaryException.compareAndSet( null, translatedException ) )
        {
            return translatedException;
        }

        return new FabricSecondaryException( translatedException.status(), translatedException.getMessage(), translatedException.getCause(),
                primaryException.get() );
    }

    private FabricException translateError( Neo4jException driverException )
    {
        // only user errors ( typically wrongly written query ) keep the original status code
        // server errors get a special status to distinguish them from error occurring on the local server
        if ( driverException instanceof ClientException )
        {
            var serverCode = Status.Code.all().stream().filter( code -> code.code().serialize().equals( driverException.code() ) ).findAny();

            if ( serverCode.isEmpty() )
            {
                return genericRemoteFailure( driverException );
            }

            return new FabricException( serverCode.get(), driverException.getMessage(), driverException );
        }

        return genericRemoteFailure( driverException );
    }

    private FabricException genericRemoteFailure( Neo4jException driverException )
    {
        return new FabricException( Status.Fabric.RemoteExecutionFailed,
                String.format( "Remote execution failed with code %s and message '%s'", driverException.code(), driverException.getMessage() ),
                driverException );
    }
}
