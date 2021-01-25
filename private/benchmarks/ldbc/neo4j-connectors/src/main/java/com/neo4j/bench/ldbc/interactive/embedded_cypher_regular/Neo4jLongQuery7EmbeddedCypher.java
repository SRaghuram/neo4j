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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery7;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jLongQuery7EmbeddedCypher extends Neo4jQuery7<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery7Result> execute( Neo4jConnectionState connection, LdbcQuery7 operation )
            throws DbException
    {
        return Lists.newArrayList( Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation ) ),
                new TransformFun( connection.dateUtil() ) ) );
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcQuery7Result>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcQuery7Result apply( Map<String,Object> row )
        {
            long messageCreationDateAsUtc = dateUtil.formatToUtc( (long) row.get( "messageCreationDate" ) );
            long likeTimeAsUtc = dateUtil.formatToUtc( (long) row.get( "likeTime" ) );
            long latencyAsMilli = likeTimeAsUtc - messageCreationDateAsUtc;
            Long latencyAsMinutes = (latencyAsMilli / 1000) / 60;
            return new LdbcQuery7Result(
                    (long) row.get( "personId" ),
                    (String) row.get( "personFirstName" ),
                    (String) row.get( "personLastName" ),
                    dateUtil.formatToUtc( (long) row.get( "likeTime" ) ),
                    (long) row.get( "messageId" ),
                    (String) row.get( "messageContent" ),
                    latencyAsMinutes.intValue(),
                    (boolean) row.get( "isNew" ) );
        }
    }

    private Map<String,Object> buildParams( LdbcQuery7 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
