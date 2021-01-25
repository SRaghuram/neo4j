/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery1;

import java.util.HashMap;
import java.util.Map;

public class Neo4jShortQuery1EmbeddedCypher extends Neo4jShortQuery1<Neo4jConnectionState>
{
    private static final String PERSON_ID_STRING = PERSON_ID.toString();

    @Override
    public LdbcShortQuery1PersonProfileResult execute( Neo4jConnectionState connection,
            LdbcShortQuery1PersonProfile operation ) throws DbException
    {
        return new TransformFun( connection.dateUtil() ).apply(
                connection.execute(
                        connection.queries().queryFor( operation ).queryString(),
                        buildParams( operation )
                ).next() );
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcShortQuery1PersonProfileResult>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcShortQuery1PersonProfileResult apply( Map<String,Object> row )
        {
            return new LdbcShortQuery1PersonProfileResult(
                    (String) row.get( "firstName" ),
                    (String) row.get( "lastName" ),
                    dateUtil.formatToUtc( (long) row.get( "birthday" ) ),
                    (String) row.get( "locationIp" ),
                    (String) row.get( "browserUsed" ),
                    (long) row.get( "cityId" ),
                    (String) row.get( "gender" ),
                    dateUtil.formatToUtc( (long) row.get( "creationDate" ) )
            );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery1PersonProfile operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        return queryParams;
    }
}
