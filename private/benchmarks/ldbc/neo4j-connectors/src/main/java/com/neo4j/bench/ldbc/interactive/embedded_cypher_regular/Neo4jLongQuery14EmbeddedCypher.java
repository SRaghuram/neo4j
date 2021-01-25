/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery14;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jLongQuery14EmbeddedCypher extends Neo4jQuery14<Neo4jConnectionState>
{
    protected static final String PERSON_ID_1_STRING = PERSON_ID_1.toString();
    protected static final String PERSON_ID_2_STRING = PERSON_ID_2.toString();

    @Override
    public List<LdbcQuery14Result> execute( Neo4jConnectionState connection, LdbcQuery14 operation )
            throws DbException
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

    private static final Function<Map<String,Object>,LdbcQuery14Result> TRANSFORM_FUN =
            new Function<>()
            {
                @Override
                public LdbcQuery14Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery14Result(
                            (Collection<Long>) row.get( "pathNodeIds" ),
                            ((Number) row.get( "weight" )).doubleValue() );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery14 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_1_STRING, operation.person1Id() );
        queryParams.put( PERSON_ID_2_STRING, operation.person2Id() );
        return queryParams;
    }
}
