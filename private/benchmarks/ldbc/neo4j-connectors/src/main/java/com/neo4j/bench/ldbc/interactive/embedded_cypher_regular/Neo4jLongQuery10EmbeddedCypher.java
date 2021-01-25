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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery10;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jLongQuery10EmbeddedCypher extends Neo4jQuery10<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String MONTH_STRING = MONTH.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery10Result> execute( Neo4jConnectionState connection, LdbcQuery10 operation )
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

    private static final Function<Map<String,Object>,LdbcQuery10Result> TRANSFORM_FUN =
            new Function<>()
            {
                @Override
                public LdbcQuery10Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery10Result(
                            ((Number) row.get( "personId" )).longValue(),
                            (String) row.get( "personFirstName" ),
                            (String) row.get( "personLastName" ),
                            ((Number) row.get( "commonInterestScore" )).intValue(),
                            (String) row.get( "personGender" ),
                            (String) row.get( "personCityName" ) );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery10 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( MONTH_STRING, operation.month() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
