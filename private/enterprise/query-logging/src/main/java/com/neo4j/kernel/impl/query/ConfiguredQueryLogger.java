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
import org.neo4j.logging.log4j.LogExtended;
import org.neo4j.logging.log4j.StructureAwareMessage;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.virtual.MapValue;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.neo4j.values.AnyValueWriter.EntityMode.FULL;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

class ConfiguredQueryLogger implements QueryLogger
{
    private enum QueryEvent
    {
        START,
        ROLLBACK,
        COMMIT
    }

    private final LogExtended log;
    private final long thresholdMillis;
    private final boolean logQueryParameters;
    private final AnyValueWriter.EntityMode parameterEntityMode;
    private final boolean logDetailedTime;
    private final boolean logAllocatedBytes;
    private final boolean logPageDetails;
    private final boolean logRuntime;
    private final boolean verboseLogging;
    private final boolean rawLogging;

    ConfiguredQueryLogger( LogExtended log, Config config )
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
            boolean canLogStart = canCreateLogEntry( snapshot );

            if ( !alreadyLoggedStart && canLogStart )
            {
                log.info( new QueryLogLine( snapshot, false, QueryEvent.START ) );
            }
        }
    }

    private boolean canCreateLogEntry( QuerySnapshot query )
    {
        return query.obfuscatedQueryText().isPresent() || rawLogging;
    }

    @Override
    public void failure( ExecutingQuery query, Throwable failure )
    {
        log.error( new QueryLogLine( query.snapshot(), true, QueryEvent.ROLLBACK ), failure );
    }

    @Override
    public void failure( ExecutingQuery query, String reason )
    {
        log.error( new QueryLogLine( query.snapshot(), true, QueryEvent.ROLLBACK, reason ) );
    }

    @Override
    public void success( ExecutingQuery query )
    {
        if ( NANOSECONDS.toMillis( query.elapsedNanos() ) >= thresholdMillis || verboseLogging )
        {
            log.info( new QueryLogLine( query.snapshot(), false, QueryEvent.COMMIT ) );
        }
    }

    private class QueryLogLine extends StructureAwareMessage
    {
        private final QuerySnapshot snapshot;
        private final QueryEvent event;
        private final String sourceString;
        private final String username;
        private final String databaseName;
        private final String reason;
        private final String queryText;
        private final boolean shouldUseRawText;

        private QueryLogLine( QuerySnapshot snapshot, boolean fallbackToRaw, QueryEvent event )
        {
            this( snapshot, fallbackToRaw, event, null );
        }

        private QueryLogLine( QuerySnapshot snapshot, boolean fallbackToRaw, QueryEvent event, String reason )
        {
            this.snapshot = snapshot;
            this.event = event;
            this.reason = reason;

            sourceString = snapshot.clientConnection().asConnectionDetails();
            username = snapshot.username();
            databaseName = snapshot.databaseId().map( NamedDatabaseId::name ).orElse( "<none>" );

            shouldUseRawText = rawLogging || (snapshot.obfuscatedQueryText().isEmpty() && fallbackToRaw);

            queryText = shouldUseRawText ? snapshot.rawQueryText()
                                         : snapshot.obfuscatedQueryText().get();
        }

        @Override
        public void asString( StringBuilder sb )
        {
            if ( event == QueryEvent.START )
            {
                sb.append( "Query started: " );
            }
            if ( verboseLogging )
            {
                sb.append( "id:" ).append( snapshot.id() ).append( " - " );
            }
            sb.append( TimeUnit.MICROSECONDS.toMillis( snapshot.elapsedTimeMicros() ) ).append( " ms: " );
            if ( logDetailedTime )
            {
                QueryLogFormatter.formatDetailedTime( sb, snapshot );
            }
            if ( logAllocatedBytes )
            {
                QueryLogFormatter.formatAllocatedBytes( sb, snapshot );
            }
            if ( logPageDetails )
            {
                QueryLogFormatter.formatPageDetails( sb, snapshot );
            }

            sb.append( sourceString ).append( "\t" ).append( databaseName ).append( " - " ).append( username ).append( " - " ).append( queryText );

            if ( logQueryParameters )
            {
                MapValue params = shouldUseRawText ? snapshot.rawQueryParameters()
                        : snapshot.obfuscatedQueryParameters().get();
                QueryLogFormatter.formatMapValue( sb.append(" - "), params, parameterEntityMode );
            }
            if ( logRuntime )
            {
                sb.append( " - runtime=" ).append( snapshot.runtime() );
            }
            QueryLogFormatter.formatMap( sb.append(" - "), snapshot.transactionAnnotationData() );
            if ( reason != null )
            {
                sb.append( " - " ).append( reason.split( System.lineSeparator() )[0] );
            }
        }

        @Override
        public void asStructure( FieldConsumer consumer )
        {
            StringBuilder buffer = new StringBuilder();
            consumer.add( "event", event.toString().toLowerCase() );
            if ( verboseLogging )
            {
                consumer.add( "id", snapshot.id() );
            }
            consumer.add( "elapsedTimeMs", TimeUnit.MICROSECONDS.toMillis( snapshot.elapsedTimeMicros() ) );
            if ( logDetailedTime )
            {
                consumer.add( "planning", TimeUnit.MICROSECONDS.toMillis( snapshot.compilationTimeMicros() ) );
                consumer.add( "cpu", TimeUnit.MICROSECONDS.toMillis( snapshot.cpuTimeMicros() ) );
                consumer.add( "waiting", TimeUnit.MICROSECONDS.toMillis( snapshot.waitTimeMicros() ) );
            }
            if ( logAllocatedBytes )
            {
                consumer.add( "allocatedBytes", snapshot.allocatedBytes() );
            }
            if ( logPageDetails )
            {
                consumer.add( "pageHits", snapshot.pageHits() );
                consumer.add( "pageFaults", snapshot.pageFaults() );
            }
            consumer.add( "source", sourceString );
            consumer.add( "database", databaseName );
            consumer.add( "username", username );
            consumer.add( "query", queryText );

            if ( logQueryParameters )
            {
                MapValue params = shouldUseRawText ? snapshot.rawQueryParameters()
                        : snapshot.obfuscatedQueryParameters().get();
                QueryLogFormatter.formatMapValue( buffer, params, parameterEntityMode );
                consumer.add( "queryParameters", buffer.toString() );
            }
            if ( logRuntime )
            {
                consumer.add( "runtime", snapshot.runtime() );
            }
            buffer.setLength( 0 );
            QueryLogFormatter.formatMap( buffer, snapshot.transactionAnnotationData() );
            consumer.add( "annotationData", buffer.toString() );
            if ( reason != null )
            {
                consumer.add( "failureReason", reason );
            }
        }
    }

}
