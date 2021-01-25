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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery7;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jShortQuery7EmbeddedCypher extends Neo4jShortQuery7<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery7MessageReplies operation ) throws DbException
    {
        return Lists.newArrayList( Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation ) ),
                new TransformFun( connection.dateUtil() ) ) );
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcShortQuery7MessageRepliesResult>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcShortQuery7MessageRepliesResult apply( Map<String,Object> row )
        {
            return new LdbcShortQuery7MessageRepliesResult(
                    (long) row.get( "replyId" ),
                    (String) row.get( "replyContent" ),
                    dateUtil.formatToUtc( (long) row.get( "replyCreationDate" ) ),
                    (long) row.get( "replyAuthorId" ),
                    (String) row.get( "replyAuthorFirstName" ),
                    (String) row.get( "replyAuthorLastName" ),
                    (boolean) row.get( "replyAuthorKnowsAuthor" )
            );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery7MessageReplies operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
