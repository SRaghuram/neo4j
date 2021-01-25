/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery4;

import java.util.HashMap;
import java.util.Map;

public class Neo4jShortQuery4EmbeddedCypher extends Neo4jShortQuery4<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public LdbcShortQuery4MessageContentResult execute( Neo4jConnectionState connection,
            LdbcShortQuery4MessageContent operation ) throws DbException
    {
        return new TransformFun( connection.dateUtil() ).apply(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation )
                ).next() );
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcShortQuery4MessageContentResult>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcShortQuery4MessageContentResult apply( Map<String,Object> row )
        {
            return new LdbcShortQuery4MessageContentResult(
                    (String) row.get( "messageContent" ),
                    dateUtil.formatToUtc( (long) row.get( "messageCreationDate" ) )
            );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery4MessageContent operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
