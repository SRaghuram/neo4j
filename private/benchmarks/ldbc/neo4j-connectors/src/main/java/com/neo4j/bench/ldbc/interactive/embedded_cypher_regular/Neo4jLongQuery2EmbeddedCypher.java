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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jLongQuery2EmbeddedCypher extends Neo4jQuery2<Neo4jConnectionState>
{
    private static final String PERSON_ID_STRING = PERSON_ID.toString();
    private static final String MAX_DATE_STRING = MAX_DATE.toString();
    private static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery2Result> execute( Neo4jConnectionState connection, LdbcQuery2 operation )
            throws DbException
    {
        return Lists.newArrayList( Iterators.transform(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation, connection.dateUtil() ) ),
                new TransformFun( connection.dateUtil() ) ) );
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcQuery2Result>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcQuery2Result apply( Map<String,Object> input )
        {
            return new LdbcQuery2Result(
                    ((Number) input.get( "personId" )).longValue(),
                    (String) input.get( "personFirstName" ),
                    (String) input.get( "personLastName" ),
                    ((Number) input.get( "messageId" )).longValue(),
                    (String) input.get( "messageContent" ),
                    dateUtil.formatToUtc( ((Number) input.get( "messageDate" )).longValue() ) );
        }
    }

    private Map<String,Object> buildParams( LdbcQuery2 operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( MAX_DATE_STRING, dateUtil.utcToFormat( operation.maxDate().getTime() ) );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
