/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery5;
import com.neo4j.bench.ldbc.utils.PlanMeta;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

public class Neo4jShortQuery5EmbeddedCypher extends Neo4jShortQuery5<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public LdbcShortQuery5MessageCreatorResult execute( Neo4jConnectionState connection,
            LdbcShortQuery5MessageCreator operation ) throws DbException
    {
        if ( connection.isFirstForType( operation.type() ) )
        {
            Result defaultPlannerResult = connection.db().execute(
                    withExplain( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            Result executionResult = connection.db().execute(
                    withProfile( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            LdbcShortQuery5MessageCreatorResult results = Iterators.transform( executionResult, TRANSFORM_FUN ).next();
            // force materialize
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
            return Iterators.transform(
                    connection.db().execute(
                            connection.queries().queryFor( operation ).queryString(),
                            buildParams( operation ) ),
                    TRANSFORM_FUN
            ).next();
        }
    }

    private static final Function<Map<String,Object>,LdbcShortQuery5MessageCreatorResult> TRANSFORM_FUN =
            new Function<Map<String,Object>,LdbcShortQuery5MessageCreatorResult>()
            {
                @Override
                public LdbcShortQuery5MessageCreatorResult apply( Map<String,Object> row )
                {
                    return new LdbcShortQuery5MessageCreatorResult(
                            (long) row.get( "personId" ),
                            (String) row.get( "personFirstName" ),
                            (String) row.get( "personLastName" )
                    );
                }
            };

    private Map<String,Object> buildParams( LdbcShortQuery5MessageCreator operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
