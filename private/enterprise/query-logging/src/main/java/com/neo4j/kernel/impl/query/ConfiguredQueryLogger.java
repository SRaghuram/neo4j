/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.virtual.MapValue;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.neo4j.values.AnyValueWriter.EntityMode.FULL;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

class ConfiguredQueryLogger implements QueryLogger
{
    private final Log log;
    private final long thresholdMillis;
    private final boolean logQueryParameters;
    private final AnyValueWriter.EntityMode parameterEntityMode;
    private final boolean logDetailedTime;
    private final boolean logAllocatedBytes;
    private final boolean logPageDetails;
    private final boolean logRuntime;
    private final boolean verboseLogging;
    private final boolean rawLogging;

    ConfiguredQueryLogger( Log log, Config config )
    {
        this.log = log;
        this.thresholdMillis = config.get( GraphDatabaseSettings.log_queries_threshold ).toMillis();
        this.logQueryParameters = config.get( GraphDatabaseSettings.log_queries_parameter_logging_enabled );
        this.parameterEntityMode = config.get( GraphDatabaseSettings.log_queries_parameter_full_entities ) ? FULL : REFERENCE;
        this.logDetailedTime = config.get( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled );
        this.logAllocatedBytes = config.get( GraphDatabaseSettings.log_queries_allocation_logging_enabled );
        this.logPageDetails = config.get( GraphDatabaseSettings.log_queries_page_detail_logging_enabled );
        this.logRuntime = config.get( GraphDatabaseSettings.log_queries_runtime_logging_enabled );
        this.verboseLogging = config.get( GraphDatabaseSettings.log_queries ) == LogQueryLevel.VERBOSE;
        this.rawLogging = config.get( GraphDatabaseSettings.log_queries_early_raw_logging_enabled );
    }

    @Override
    public void start( ExecutingQuery query )
    {
        if ( verboseLogging )
        {
            QuerySnapshot snapshot = query.snapshot();
            boolean alreadyLoggedStart = this.rawLogging && snapshot.obfuscatedQueryText().isPresent();

            if ( !alreadyLoggedStart )
            {
                log.info( "Query started: " + logEntry( snapshot, false ) );
            }
        }
    }

    @Override
    public void failure( ExecutingQuery query, Throwable failure )
    {
        log.error( logEntry( query.snapshot(), true ), failure );
    }

    @Override
    public void failure( ExecutingQuery query, String reason )
    {
        log.error( logEntry( query.snapshot(), true, reason ) );
    }

    @Override
    public void success( ExecutingQuery query )
    {
        if ( NANOSECONDS.toMillis( query.elapsedNanos() ) >= thresholdMillis || verboseLogging )
        {
            log.info( logEntry( query.snapshot(), false ) );
        }
    }

    private String logEntry( QuerySnapshot query, Boolean fallbackToRaw, String reason )
    {
        return logEntry( query, fallbackToRaw ) + " - " + reason.split( System.getProperty( "line.separator" ) )[0];
    }

    private String logEntry( QuerySnapshot query, Boolean fallbackToRaw )
    {
        String sourceString = query.clientConnection().asConnectionDetails();
        String username = query.username();
        NamedDatabaseId namedDatabaseId = query.databaseId();

        boolean shouldUseRawText = rawLogging || (query.obfuscatedQueryText().isEmpty() && fallbackToRaw);

        String queryText = shouldUseRawText ? query.rawQueryText()
                                            : query.obfuscatedQueryText().get();

        StringBuilder result = new StringBuilder( 80 );
        if ( verboseLogging )
        {
            result.append( "id:" ).append( query.id() ).append( " - " );
        }
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
        result.append( sourceString ).append( "\t" ).append( namedDatabaseId.name() ).append( " - " ).append( username ).append( " - " ).append( queryText );
        if ( logQueryParameters )
        {
            MapValue params = shouldUseRawText ? query.rawQueryParameters()
                                               : query.obfuscatedQueryParameters().get();
            QueryLogFormatter.formatMapValue( result.append(" - "), params, parameterEntityMode );
        }
        if ( logRuntime )
        {
            result.append( " - runtime=" ).append( query.runtime() );
        }
        QueryLogFormatter.formatMap( result.append(" - "), query.transactionAnnotationData() );
        return result.toString();
    }
}
