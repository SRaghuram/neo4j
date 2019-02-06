/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery3;
import com.neo4j.bench.ldbc.utils.PlanMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

public class Neo4jLongQuery3EmbeddedCypher extends Neo4jQuery3<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String COUNTRY_X_STRING = COUNTRY_X.toString();
    protected static final String COUNTRY_Y_STRING = COUNTRY_Y.toString();
    protected static final String MIN_DATE_STRING = MIN_DATE.toString();
    protected static final String MAX_DATE_STRING = MAX_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery3Result> execute( Neo4jConnectionState connection, LdbcQuery3 operation )
            throws DbException
    {
        if ( connection.isFirstForType( operation.type() ) )
        {
            Result defaultPlannerResult = connection.db().execute(
                    withExplain( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation, connection.dateUtil() ) );
            Result executionResult = connection.db().execute(
                    withProfile( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation, connection.dateUtil() ) );
            List<LdbcQuery3Result> results =
                    ImmutableList.copyOf( Iterators.transform( executionResult, TRANSFORM_FUN ) );
            // force materialize
            results.size();
            connection.reportPlanStats(
                    operation,
                    PlanMeta.extractPlanner( defaultPlannerResult.getExecutionPlanDescription() ),
                    PlanMeta.extractPlanner( executionResult.getExecutionPlanDescription() ),
                    executionResult.getExecutionPlanDescription()
            );
            return results;
        }
        else
        {
            return Lists.newArrayList( Iterators.transform(
                    connection.db().execute(
                            connection.queries().queryFor( operation ).queryString(),
                            buildParams( operation, connection.dateUtil() ) ),
                    TRANSFORM_FUN ) );
        }
    }

    private static final Function<Map<String,Object>,LdbcQuery3Result> TRANSFORM_FUN =
            new Function<Map<String,Object>,LdbcQuery3Result>()
            {
                @Override
                public LdbcQuery3Result apply( Map<String,Object> cypherResult )
                {
                    return new LdbcQuery3Result(
                            (long) cypherResult.get( "friendId" ),
                            (String) cypherResult.get( "friendFirstName" ),
                            (String) cypherResult.get( "friendLastName" ),
                            (long) cypherResult.get( "xCount" ),
                            (long) cypherResult.get( "yCount" ),
                            (long) cypherResult.get( "xyCount" ) );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery3 operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( COUNTRY_X_STRING, operation.countryXName() );
        queryParams.put( COUNTRY_Y_STRING, operation.countryYName() );
        long startDateAsUtc = operation.startDate().getTime();
        long endDateAsUtc = startDateAsUtc + TimeUnit.DAYS.toMillis( operation.durationDays() );
        queryParams.put( MIN_DATE_STRING, dateUtil.utcToFormat( startDateAsUtc ) );
        queryParams.put( MAX_DATE_STRING, dateUtil.utcToFormat( endDateAsUtc ) );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
