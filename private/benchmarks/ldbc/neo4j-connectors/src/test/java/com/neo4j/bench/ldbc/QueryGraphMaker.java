/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple2;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static java.lang.String.format;

public abstract class QueryGraphMaker
{
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
            queries.add( "CREATE INDEX ON :" + labelAndProperty._1() + "(" + labelAndProperty._2() + ")" );
        }
        return queries;
    }

    public static void createDbFromQueryGraphMaker(
            QueryGraphMaker queryGraphMaker,
            String dbDir,
            Neo4jSchema neo4jSchema ) throws Exception
    {
        System.out.println();
        System.out.println( MapUtils.prettyPrint( queryGraphMaker.params() ) );
        System.out.println( queryGraphMaker.queryString() );
        GraphDatabaseService db = Neo4jDb.newDb(
                new File( dbDir ),
                DriverConfigUtils.neo4jTestConfig() );
        createDbFromCypherQuery(
                db,
                queryGraphMaker.queryString(),
                queryGraphMaker.params(),
                neo4jSchema );
        LdbcIndexer.waitForIndexesToBeOnline( db );
        db.shutdown();
    }

    public static void createDbFromCypherQuery(
            GraphDatabaseService db,
            String createQuery,
            Map<String,Object> queryParams,
            Neo4jSchema neo4jSchema )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( createQuery, queryParams );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw e;
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( String createIndexQuery : createIndexQueries( neo4jSchema ) )
            {
                System.out.println( createIndexQuery );
                db.execute( createIndexQuery );
            }
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
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
        Calendar calendar = LdbcDateCodec.newCalendar();
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
        Calendar calendar = LdbcDateCodec.newCalendar();
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
        Calendar calendar = LdbcDateCodec.newCalendar();
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
        Calendar calendar = LdbcDateCodec.newCalendar();
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
