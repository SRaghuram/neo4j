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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery6;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jLongQuery6EmbeddedCypher extends Neo4jQuery6<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String TAG_NAME_STRING = TAG_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery6Result> execute( Neo4jConnectionState connection, LdbcQuery6 operation )
            throws DbException
    {
        return Lists.newArrayList( Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation ) ),
                TRANSFORM_FUN ) );
    }

    private static final Function<Map<String,Object>,LdbcQuery6Result> TRANSFORM_FUN =
            new Function<>()
            {
                @Override
                public LdbcQuery6Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery6Result(
                            (String) row.get( "tagName" ),
                            ((Long) row.get( "postCount" )).intValue() );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery6 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( TAG_NAME_STRING, operation.tagName() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
