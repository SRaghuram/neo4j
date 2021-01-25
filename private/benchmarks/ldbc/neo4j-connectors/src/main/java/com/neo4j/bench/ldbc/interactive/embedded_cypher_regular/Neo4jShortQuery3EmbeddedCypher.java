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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jShortQuery3EmbeddedCypher extends Neo4jShortQuery3<Neo4jConnectionState>
{
    private static final String PERSON_ID_STRING = PERSON_ID.toString();

    @Override
    public List<LdbcShortQuery3PersonFriendsResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery3PersonFriends operation ) throws DbException
    {
        return Lists.newArrayList( Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation )
                ),
                new TransformFun( connection.dateUtil() ) ) );
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcShortQuery3PersonFriendsResult>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcShortQuery3PersonFriendsResult apply( Map<String,Object> row )
        {
            return new LdbcShortQuery3PersonFriendsResult(
                    (long) row.get( "friendId" ),
                    (String) row.get( "friendFirstName" ),
                    (String) row.get( "friendLastName" ),
                    dateUtil.formatToUtc( (long) row.get( "knowsCreationDate" ) )
            );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery3PersonFriends operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        return queryParams;
    }
}
