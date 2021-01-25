/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery13;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jLongQuery13EmbeddedCypher extends Neo4jQuery13<Neo4jConnectionState>
{
    protected static final String PERSON_ID_1_STRING = PERSON_ID_1.toString();
    protected static final String PERSON_ID_2_STRING = PERSON_ID_2.toString();

    @Override
    public LdbcQuery13Result execute( Neo4jConnectionState connection, LdbcQuery13 operation ) throws DbException
    {
        Result result = connection.execute(
                connection.queries().queryFor( operation ).queryString(),
                buildParams( operation ) );
        if ( !result.hasNext() )
        {
            throw new RuntimeException( "Expected 1 row got 0" );
        }
        return Iterators.transform(
                result,
                TRANSFORM_FUN ).next();
    }

private static final Function<Map<String,Object>,LdbcQuery13Result> TRANSFORM_FUN =
        new Function<>()
        {
            @Override
            public LdbcQuery13Result apply( Map<String,Object> row )
            {
                return new LdbcQuery13Result(
                        ((Number) row.get( "pathLength" )).intValue() );
            }
        };

    private Map<String,Object> buildParams( LdbcQuery13 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_1_STRING, operation.person1Id() );
        queryParams.put( PERSON_ID_2_STRING, operation.person2Id() );
        return queryParams;
    }
}
