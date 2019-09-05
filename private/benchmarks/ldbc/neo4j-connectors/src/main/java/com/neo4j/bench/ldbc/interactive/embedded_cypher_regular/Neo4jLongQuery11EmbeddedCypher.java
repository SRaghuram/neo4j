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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery11;
import com.neo4j.bench.ldbc.utils.PlanMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

public class Neo4jLongQuery11EmbeddedCypher extends Neo4jQuery11<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String WORK_FROM_YEAR_STRING = WORK_FROM_YEAR.toString();
    protected static final String COUNTRY_NAME_STRING = COUNTRY_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery11Result> execute( Neo4jConnectionState connection, LdbcQuery11 operation )
            throws DbException
    {
        if ( connection.isFirstForType( operation.type() ) )
        {
            Result defaultPlannerResult = connection.execute(
                    withExplain( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            List<LdbcQuery11Result> explainResults =
                    ImmutableList.copyOf( Iterators.transform( defaultPlannerResult, TRANSFORM_FUN ) );
            // force materialize
            explainResults.size();
            Result executionResult = connection.execute(
                    withProfile( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            List<LdbcQuery11Result> results =
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
            return Lists.newArrayList(
                    Iterators.transform(
                            connection.execute(
                                    connection.queries().queryFor( operation ).queryString(),
                                    buildParams( operation ) ),
                            TRANSFORM_FUN
                    )
            );
        }
    }

    private static final Function<Map<String,Object>,LdbcQuery11Result> TRANSFORM_FUN =
            new Function<>()
            {
                @Override
                public LdbcQuery11Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery11Result(
                            ((Number) row.get( "friendId" )).longValue(),
                            (String) row.get( "friendFirstName" ),
                            (String) row.get( "friendLastName" ),
                            (String) row.get( "companyName" ),
                            ((Number) row.get( "workFromYear" )).intValue() );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery11 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( COUNTRY_NAME_STRING, operation.countryName() );
        queryParams.put( WORK_FROM_YEAR_STRING, operation.workFromYear() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
