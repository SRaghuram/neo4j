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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jShortQuery2EmbeddedCypher extends Neo4jShortQuery2<Neo4jConnectionState>
{
    private static final String PERSON_ID_STRING = PERSON_ID.toString();
    private static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcShortQuery2PersonPostsResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery2PersonPosts operation ) throws DbException
    {
        return Lists.newArrayList( Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation )
                ),
                new TransformFun( connection.dateUtil() ) ) );
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcShortQuery2PersonPostsResult>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcShortQuery2PersonPostsResult apply( Map<String,Object> row )
        {
            return new LdbcShortQuery2PersonPostsResult(
                    (long) row.get( "messageId" ),
                    (String) row.get( "messageContent" ),
                    dateUtil.formatToUtc( (long) row.get( "messageCreationDate" ) ),
                    (long) row.get( "postId" ),
                    (long) row.get( "personId" ),
                    (String) row.get( "personFirstName" ),
                    (String) row.get( "personLastName" )
            );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery2PersonPosts operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
