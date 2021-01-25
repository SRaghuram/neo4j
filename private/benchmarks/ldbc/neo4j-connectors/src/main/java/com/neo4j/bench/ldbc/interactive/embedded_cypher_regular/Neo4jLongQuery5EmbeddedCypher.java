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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery5;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jLongQuery5EmbeddedCypher extends Neo4jQuery5<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String JOIN_DATE_STRING = JOIN_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery5Result> execute( Neo4jConnectionState connection, LdbcQuery5 operation )
            throws DbException
    {
        return Lists.newArrayList( Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation, connection.dateUtil() ) ),
                TRANSFORM_FUN ) );
    }

    private static final Function<Map<String,Object>,LdbcQuery5Result> TRANSFORM_FUN =
            new Function<>()
            {
                @Override
                public LdbcQuery5Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery5Result(
                            (String) row.get( "forumName" ),
                            ((Long) row.get( "postCount" )).intValue()
                    );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery5 operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( JOIN_DATE_STRING, dateUtil.utcToFormat( operation.minDate().getTime() ) );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
