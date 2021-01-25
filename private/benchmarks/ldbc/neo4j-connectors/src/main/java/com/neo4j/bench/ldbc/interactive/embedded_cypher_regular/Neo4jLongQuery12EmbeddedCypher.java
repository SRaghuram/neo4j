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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery12;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jLongQuery12EmbeddedCypher extends Neo4jQuery12<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String TAG_CLASS_NAME_STRING = TAG_CLASS_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery12Result> execute( Neo4jConnectionState connection, LdbcQuery12 operation )
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

    private static final Function<Map<String,Object>,LdbcQuery12Result> TRANSFORM_FUN =
            new Function<>()
            {
                @Override
                public LdbcQuery12Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery12Result(
                            (long) row.get( "friendId" ),
                            (String) row.get( "friendFirstName" ),
                            (String) row.get( "friendLastName" ),
                            (Collection<String>) row.get( "tagNames" ),
                            ((Long) row.get( "count" )).intValue() );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery12 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( TAG_CLASS_NAME_STRING, operation.tagClassName() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
