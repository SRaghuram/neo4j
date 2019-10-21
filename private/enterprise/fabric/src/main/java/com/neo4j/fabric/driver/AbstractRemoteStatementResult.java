/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.Summary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.kernel.api.exceptions.Status;

abstract class AbstractRemoteStatementResult implements StatementResult
{

    private final Flux<String> columns;
    private final Mono<ResultSummary> summary;
    private final RecordConverter recordConverter;
    private final Runnable completionListener;
    private boolean completionInvoked;

    AbstractRemoteStatementResult( Flux<String> columns, Mono<ResultSummary> summary, long sourceTag )
    {
        this( columns, summary, sourceTag, () ->
        {
        } );
    }

    AbstractRemoteStatementResult( Flux<String> columns, Mono<ResultSummary> summary, long sourceTag, Runnable completionListener )
    {
        this.columns = columns;
        this.summary = summary;
        recordConverter = new RecordConverter( sourceTag );
        this.completionListener = completionListener;
    }

    @Override
    public Flux<String> columns()
    {
        return columns.onErrorMap( Neo4jException.class, this::translateError ).doOnError( ignored -> invokeCompletionListener() );
    }

    @Override
    public Flux<Record> records()
    {
        return doGetRecords()
                .onErrorMap( Neo4jException.class, this::translateError )
                .doFinally( signalType -> invokeCompletionListener() );
    }

    @Override
    public Mono<Summary> summary()
    {
        return summary.onErrorMap( Neo4jException.class, this::translateError )
            .map( ResultSummaryWrapper::new );
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

            return new FabricException( serverCode.get(), driverException.getMessage() );
        }

        return genericRemoteFailure( driverException );
    }

    private FabricException genericRemoteFailure( Neo4jException driverException )
    {
        throw new FabricException( Status.Fabric.RemoteExecutionFailed,
                "Remote execution failed with code %s and message '%s'", driverException.code(), driverException.getMessage() );
    }
}
