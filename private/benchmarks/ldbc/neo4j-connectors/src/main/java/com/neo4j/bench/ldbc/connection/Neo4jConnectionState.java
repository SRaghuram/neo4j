/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static java.lang.String.format;
import static java.lang.ThreadLocal.withInitial;

public class Neo4jConnectionState extends DbConnectionState
{
    private final ThreadLocal<QueryDateUtil> dateUtilThreadLocal;
    private final ThreadLocal<Calendar> calendarThreadLocal;

    private final DatabaseManagementService managementService;
    private final GraphDatabaseService db;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;
    private final DriverSupplier driverSupplier;

    private final AnnotatedQueries queries;

    private ThreadLocal<Transaction> threadLocalTx = ThreadLocal.withInitial( () -> null );

    public Neo4jConnectionState( DatabaseManagementService managementService, GraphDatabaseService db, URI uri, AuthToken authToken,
            LoggingService loggingService, AnnotatedQueries annotatedQueries, final LdbcDateCodec.Format dateFormat,
            final LdbcDateCodec.Resolution timestampResolution )
    {
        this.managementService = managementService;
        this.db = db;
        this.driverSupplier = new DriverSupplier( new LoggingServiceFactoryWrapper( loggingService ), uri, authToken );
        this.timeStampedRelationshipTypesCache = new TimeStampedRelationshipTypesCache();
        this.queries = annotatedQueries;
        this.dateUtilThreadLocal = withInitial( () ->
                QueryDateUtil.createFor( dateFormat, timestampResolution, new LdbcDateCodecUtil() ) );
        this.calendarThreadLocal = withInitial( LdbcDateCodecUtil::newCalendar );
    }

    public Session session()
    {
        return driverSupplier.get().session();
    }

    public GraphDatabaseService getDb()
    {
        return db;
    }

    public Transaction beginTx()
    {
        if ( null != threadLocalTx.get() )
        {
            throw new RuntimeException( "There is already an open transaction!" );
        }
        Transaction tx = db.beginTx();
        threadLocalTx.set( tx );
        return tx;
    }

    public Transaction getTx()
    {
        Transaction tx = threadLocalTx.get();
        if ( null == tx )
        {
            throw new RuntimeException( "There is no open transaction!" );
        }
        return tx;
    }

    public void freeTx()
    {
        if ( null == threadLocalTx.get() )
        {
            throw new RuntimeException( "There is no open transaction!" );
        }
        threadLocalTx.set( null );
    }

    public Result execute( String query, Map<String,Object> parameters )
    {
        return threadLocalTx.get().execute( query, parameters );
    }

    public TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache()
    {
        return timeStampedRelationshipTypesCache;
    }

    public AnnotatedQueries queries()
    {
        return queries;
    }

    public QueryDateUtil dateUtil()
    {
        return dateUtilThreadLocal.get();
    }

    public Calendar calendar()
    {
        return calendarThreadLocal.get();
    }

    private static class LoggingServiceFactoryWrapper implements Logging
    {
        private final LoggingService loggingService;

        LoggingServiceFactoryWrapper( LoggingService loggingService )
        {
            this.loggingService = loggingService;
        }

        @Override
        public Logger getLog( String s )
        {
            return new LoggingServiceWrapper( loggingService );
        }
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            driverSupplier.close();
        }
        catch ( Exception e )
        {
            throw new IOException( "Error while closing driver connection", e );
        }

        if ( null != managementService )
        {
            managementService.shutdown();
        }
    }

    private static class LoggingServiceWrapper implements Logger
    {
        private final LoggingService loggingService;
        private final Logger logger;

        LoggingServiceWrapper( LoggingService loggingService )
        {
            this.loggingService = loggingService;
            this.logger = (Logger.class.isAssignableFrom( loggingService.getClass() ))
                          ? (Logger) loggingService
                          : null;
        }

        @Override
        public void error( String s, Throwable throwable )
        {
            if ( null == logger )
            {
                loggingService.info( format( "%s\n%s", s, ConcurrentErrorReporter.stackTraceToString( throwable ) ) );
            }
            else
            {
                logger.error( s, throwable );
            }
        }

        @Override
        public void info( String s, Object... objects )
        {
            if ( null == logger )
            {
                loggingService.info( format( s, objects ) );
            }
            else
            {
                logger.info( s, objects );
            }
        }

        @Override
        public void warn( String s, Object... objects )
        {
//            if ( null == logger )
//            {
//                loggingService.info( format( s, objects ) );
//            }
//            else
//            {
//                logger.warn( s, objects );
//            }
        }

        @Override
        public void warn( String message, Throwable cause )
        {

        }

        @Override
        public void debug( String s, Object... objects )
        {
//            if ( null == logger )
//            {
//                loggingService.info( format( s, objects ) );
//            }
//            else
//            {
//                logger.debug( s, objects );
//            }
        }

        @Override
        public void trace( String s, Object... objects )
        {
//            if ( null == logger )
//            {
//                loggingService.info( format( s, objects ) );
//            }
//            else
//            {
//                logger.trace( s, objects );
//            }
        }

        @Override
        public boolean isTraceEnabled()
        {
            return true;
        }

        @Override
        public boolean isDebugEnabled()
        {
            return true;
        }
    }
}
