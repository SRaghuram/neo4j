/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.query;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

class ConfiguredQueryLogger implements QueryLogger
{
    private final Log log;
    private final long thresholdMillis;
    private final boolean logQueryParameters;
    private final boolean logDetailedTime;
    private final boolean logAllocatedBytes;
    private final boolean logPageDetails;
    private final boolean logRuntime;

    ConfiguredQueryLogger( Log log, Config config )
    {
        this.log = log;
        this.thresholdMillis = config.get( GraphDatabaseSettings.log_queries_threshold ).toMillis();
        this.logQueryParameters = config.get( GraphDatabaseSettings.log_queries_parameter_logging_enabled );
        this.logDetailedTime = config.get( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled );
        this.logAllocatedBytes = config.get( GraphDatabaseSettings.log_queries_allocation_logging_enabled );
        this.logPageDetails = config.get( GraphDatabaseSettings.log_queries_page_detail_logging_enabled );
        this.logRuntime = config.get( GraphDatabaseSettings.log_queries_runtime_logging_enabled );
    }

    @Override
    public void failure( ExecutingQuery query, Throwable failure )
    {
        log.error( logEntry( query.snapshot() ), failure );
    }

    @Override
    public void success( ExecutingQuery query )
    {
        if ( NANOSECONDS.toMillis( query.elapsedNanos() ) >= thresholdMillis )
        {
            QuerySnapshot snapshot = query.snapshot();
            log.info( logEntry( snapshot ) );
        }
    }

    private String logEntry( QuerySnapshot query )
    {
        String sourceString = query.clientConnection().asConnectionDetails();
        String queryText = query.queryText();

        StringBuilder result = new StringBuilder();
        result.append( TimeUnit.MICROSECONDS.toMillis( query.elapsedTimeMicros() ) ).append( " ms: " );
        if ( logDetailedTime )
        {
            QueryLogFormatter.formatDetailedTime( result, query );
        }
        if ( logAllocatedBytes )
        {
            QueryLogFormatter.formatAllocatedBytes( result, query );
        }
        if ( logPageDetails )
        {
            QueryLogFormatter.formatPageDetails( result, query );
        }
        result.append( sourceString ).append( " - " ).append( queryText );
        if ( logQueryParameters )
        {
            QueryLogFormatter.formatMapValue( result.append(" - "), query.queryParameters() );
        }
        if ( logRuntime )
        {
            result.append( " - runtime=" ).append( query.runtime() );
        }
        QueryLogFormatter.formatMap( result.append(" - "), query.transactionAnnotationData() );
        return result.toString();
    }
}
