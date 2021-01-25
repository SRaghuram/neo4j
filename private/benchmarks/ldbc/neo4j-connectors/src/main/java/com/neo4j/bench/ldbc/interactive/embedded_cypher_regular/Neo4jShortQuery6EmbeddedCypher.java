/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery6;

import java.util.HashMap;
import java.util.Map;

public class Neo4jShortQuery6EmbeddedCypher extends Neo4jShortQuery6<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public LdbcShortQuery6MessageForumResult execute( Neo4jConnectionState connection,
            LdbcShortQuery6MessageForum operation ) throws DbException
    {
        return Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation ) ),
                TRANSFORM_FUN
        ).next();
    }

    private static final Function<Map<String,Object>,LdbcShortQuery6MessageForumResult> TRANSFORM_FUN =
            new Function<>()
            {
                @Override
                public LdbcShortQuery6MessageForumResult apply( Map<String,Object> row )
                {
                    return new LdbcShortQuery6MessageForumResult(
                            (long) row.get( "forumId" ),
                            (String) row.get( "forumTitle" ),
                            (long) row.get( "moderatorId" ),
                            (String) row.get( "moderatorFirstName" ),
                            (String) row.get( "moderatorLastName" )
                    );
                }
            };

    private Map<String,Object> buildParams( LdbcShortQuery6MessageForum operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
