/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple2;
import com.neo4j.bench.ldbc.connection.LdbcDateCodecUtil;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public abstract class QueryGraphMaker
{
    private static final Logger LOG = LoggerFactory.getLogger( QueryGraphMaker.class );

    public abstract String queryString();

    public abstract Map<String,Object> params();

    private static Iterable<String> createIndexQueries( Neo4jSchema neo4jSchema )
    {
        Set<Tuple2<Label,String>> toUnique = Domain.toUnique( neo4jSchema );
        Set<Tuple2<Label,String>> indexes = Domain.toIndex( neo4jSchema, toUnique );
        List<String> queries = new ArrayList<>();
        for ( Tuple2<Label,String> labelAndProperty : toUnique )
        {
            queries.add(
                    "CREATE CONSTRAINT ON (node:" + labelAndProperty._1() + ") ASSERT node." + labelAndProperty._2() +
                    " IS UNIQUE" );
        }
        for ( Tuple2<Label,String> labelAndProperty : indexes )
        {
            queries.add( "CREATE INDEX FOR (n:" + labelAndProperty._1() + ") ON (n." + labelAndProperty._2() + ")" );
        }
        return queries;
    }

    public static void createDbFromQueryGraphMaker(
            QueryGraphMaker queryGraphMaker,
            File dbDir,
            Neo4jSchema neo4jSchema,
            File configDir ) throws Exception
    {
        LOG.debug( MapUtils.prettyPrint( queryGraphMaker.params() ) );
        LOG.debug( queryGraphMaker.queryString() );
        DatabaseManagementService managementService = Neo4jDb.newDb( dbDir, DriverConfigUtils.neo4jTestConfig( configDir ) );
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        createDbFromCypherQuery(
                db,
                queryGraphMaker.queryString(),
                queryGraphMaker.params(),
                neo4jSchema );
        LdbcIndexer.waitForIndexesToBeOnline( db );
        managementService.shutdown();
    }

    public static void createDbFromCypherQuery(
            GraphDatabaseService db,
            String createQuery,
            Map<String,Object> queryParams,
            Neo4jSchema neo4jSchema )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( createQuery, queryParams );
            tx.commit();
        }
        catch ( Exception e )
        {
            LOG.error( "fatal error", e );
            throw e;
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( String createIndexQuery : createIndexQueries( neo4jSchema ) )
            {
                LOG.debug( createIndexQuery );
                tx.execute( createIndexQuery );
            }
            tx.commit();
        }
        catch ( Exception e )
        {
            LOG.error( "fatal error", e );
            throw e;
        }
    }

    public static long duration( int years )
    {
        return duration( years, 0 );
    }

    public static long duration( int years, int months )
    {
        return duration( years, months, 0 );
    }

    public static long duration( int years, int months, int days )
    {
        return TimeUnit.DAYS.toMillis( days ) +
               TimeUnit.DAYS.toMillis( months * 30 ) +
               TimeUnit.DAYS.toMillis( years * 365 );
    }

    public static long date(
            int year )
    {
        return date(
                year,
                1
        );
    }

    public static long date(
            int year,
            int month )
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        return date(
                year,
                month,
                calendar.getActualMinimum( Calendar.DAY_OF_MONTH )
        );
    }

    public static long date(
            int year,
            int month,
            int day )
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        return date(
                year,
                month,
                day,
                calendar.getActualMinimum( Calendar.HOUR_OF_DAY ),
                calendar.getActualMinimum( Calendar.MINUTE ),
                calendar.getActualMinimum( Calendar.SECOND )
        );
    }

    public static long date(
            int year,
            int month, int day, int hour, int minute, int second )
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        return date(
                year,
                month,
                day,
                hour,
                minute,
                second,
                calendar.getActualMinimum( Calendar.MILLISECOND )
        );
    }

    public static long date( int year, int month, int day, int hour, int minute, int second, int milliseconds )
    {
        if ( month < 1 || month > 12 )
        {
            throw new RuntimeException( format( "Invalid 'month' value %s, must be in [1,12]", month ) );
        }
        month = month - 1;
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, year );
        calendar.set( Calendar.MONTH, month );
        calendar.set( Calendar.DAY_OF_MONTH, day );
        calendar.set( Calendar.HOUR_OF_DAY, hour );
        calendar.set( Calendar.MINUTE, minute );
        calendar.set( Calendar.SECOND, second );
        calendar.set( Calendar.MILLISECOND, milliseconds );
        return calendar.getTimeInMillis();
    }
}
