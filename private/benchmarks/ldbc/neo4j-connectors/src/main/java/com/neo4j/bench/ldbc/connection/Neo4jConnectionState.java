/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import com.google.common.collect.Lists;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;
import com.neo4j.bench.ldbc.utils.AnnotatedQuery;
import com.neo4j.bench.ldbc.utils.PlanMeta;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

import static java.lang.String.format;
import static java.lang.ThreadLocal.withInitial;

public class Neo4jConnectionState extends DbConnectionState
{
    private final ThreadLocal<QueryDateUtil> dateUtilThreadLocal;
    private final ThreadLocal<Calendar> calendarThreadLocal;

    private final GraphDatabaseService db;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;
    private final DriverSupplier driverSupplier;

    private final AnnotatedQueries queries;
    private final IntSet seenOperationTypes;
    private final Int2ObjectMap<ExecutionPlanDescription> planMap;
    private final Int2ObjectMap<PlanMeta> planMetaMap;

    public Neo4jConnectionState(
            GraphDatabaseService db,
            URI uri,
            AuthToken authToken,
            LoggingService loggingService,
            AnnotatedQueries annotatedQueries,
            final LdbcDateCodec.Format dateFormat,
            final LdbcDateCodec.Resolution timestampResolution ) throws DbException
    {
        this.db = db;
        this.driverSupplier = new DriverSupplier( new LoggingServiceFactoryWrapper( loggingService ), uri, authToken );
        this.timeStampedRelationshipTypesCache = new TimeStampedRelationshipTypesCache();
        this.queries = annotatedQueries;
        this.seenOperationTypes = new IntOpenHashSet();
        this.planMap = new Int2ObjectOpenHashMap<>();
        this.planMetaMap = new Int2ObjectOpenHashMap<>();
        this.dateUtilThreadLocal = withInitial( () ->
                QueryDateUtil.createFor( dateFormat, timestampResolution, new LdbcDateCodecUtil() ) );
        this.calendarThreadLocal = withInitial( LdbcDateCodecUtil::newCalendar );
    }

    public GraphDatabaseService db()
    {
        return db;
    }

    public Session session()
    {
        return driverSupplier.get().session();
    }

    public TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache()
    {
        return timeStampedRelationshipTypesCache;
    }

    public AnnotatedQueries queries()
    {
        return queries;
    }

    public boolean isFirstForType( int operationType )
    {
        return !seenOperationTypes.contains( operationType ) &&
               trySetOperationTypeToSeen( operationType );
    }

    public boolean hasPlanStatsFor( int operationType )
    {
        return planMetaMap.containsKey( operationType );
    }

    public <T> T reportPlanStats(
            Operation<T> operation,
            AnnotatedQuery annotatedQuery,
            Map<String,Object> params,
            Function<Result,T> resultMapper ) throws DbException
    {

        Result defaultResult = db().execute( withExplain( annotatedQuery.defaultQueryString() ), params );
        // exhaust result
        Lists.newArrayList( defaultResult ).size();
        String defaultPlanner = PlanMeta.extractPlanner( defaultResult.getExecutionPlanDescription() );

        Result result = db().execute( withProfile( annotatedQuery.queryString() ), params );
        // exhaust result
        T mappedResults = resultMapper.apply( result );
        ExecutionPlanDescription plan = result.getExecutionPlanDescription();
        String usedPlanner = PlanMeta.extractPlanner( plan );

        reportPlanStats( operation, defaultPlanner, usedPlanner, plan );

        return mappedResults;
    }

    public void reportPlanStats(
            Operation operation,
            String defaultPlanner,
            String usedPlanner,
            ExecutionPlanDescription plan ) throws DbException
    {
        if ( !planMetaMap.containsKey( operation.type() ) )
        {
            PlanMeta planMeta = new PlanMeta();
            planMeta.setDefaultPlanner( defaultPlanner );
            planMeta.setRequestedPlanner( queries.queryFor( operation ).plannerType().name() );
            planMeta.setUsedPlanner( usedPlanner );
            planMetaMap.put( operation.type(), planMeta );
            planMap.put( operation.type(), plan );
        }
    }

    public PlanMeta planMetaFor( int operationType )
    {
        if ( planMetaMap.containsKey( operationType ) )
        {
            return planMetaMap.get( operationType );
        }
        else
        {
            return null;
        }
    }

    public ExecutionPlanDescription planDescriptionFor( int operationType )
    {
        if ( planMap.containsKey( operationType ) )
        {
            return planMap.get( operationType );
        }
        else
        {
            return null;
        }
    }

    public QueryDateUtil dateUtil()
    {
        return dateUtilThreadLocal.get();
    }

    public Calendar calendar()
    {
        return calendarThreadLocal.get();
    }

    private synchronized boolean trySetOperationTypeToSeen( int operationType )
    {
        return seenOperationTypes.add( operationType );
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

        if ( null != db )
        {
            db.shutdown();
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
