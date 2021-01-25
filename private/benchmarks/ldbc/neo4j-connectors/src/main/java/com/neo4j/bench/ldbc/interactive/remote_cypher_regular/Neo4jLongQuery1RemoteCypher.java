/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;

import static java.lang.String.format;

public class Neo4jLongQuery1RemoteCypher extends Neo4jQuery1<Neo4jConnectionState>
{
    private static final String PERSON_ID_STRING = PERSON_ID.toString();
    private static final String FRIEND_FIRST_NAME_STRING = FRIEND_FIRST_NAME.toString();
    private static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery1Result> execute( Neo4jConnectionState connection, LdbcQuery1 operation )
            throws DbException
    {
        List<LdbcQuery1Result> result = new ArrayList<>( operation.limit() );
        QueryDateUtil dateUtil = connection.dateUtil();
        try ( Session session = connection.session() )
        {
            Result statementResult = session.run(
                    connection.queries().queryFor( operation ).queryString(),
                    buildParams( operation )
            );
            while ( statementResult.hasNext() )
            {
                Record record = statementResult.next();
                result.add(
                        new LdbcQuery1Result(
                                record.get( "id" ).asLong(),
                                record.get( "lastName" ).asString(),
                                record.get( "distance" ).asNumber().intValue(),
                                dateUtil.formatToUtc( record.get( "birthday" ).asLong() ),
                                dateUtil.formatToUtc( record.get( "creationDate" ).asLong() ),
                                record.get( "gender" ).asString(),
                                record.get( "browser" ).asString(),
                                record.get( "locationIp" ).asString(),
                                record.get( "emails" ).asList( LIST_VALUE_TO_STRING_LIST_FUN ),
                                record.get( "languages" ).asList( LIST_VALUE_TO_STRING_LIST_FUN ),
                                record.get( "cityName" ).asString(),
                                record.get( "unis" ).asList( LIST_VALUE_TO_3_TUPLE_LIST_FUN ),
                                record.get( "companies" ).asList( LIST_VALUE_TO_3_TUPLE_LIST_FUN )
                        )
                );
            }
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Error Executing: %s", operation ), e );
        }
        return result;
    }

    private static final Function<Value,String> LIST_VALUE_TO_STRING_LIST_FUN =
            value -> value.asString();

    private static final Function<Value,List<Object>> LIST_VALUE_TO_3_TUPLE_LIST_FUN =
            listValue -> Lists.newArrayList(
                    listValue.get( 0 ).asString(),
                    listValue.get( 1 ).asInt(),
                    listValue.get( 2 ).asString()
            );

    private Map<String,Object> buildParams( LdbcQuery1 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( FRIEND_FIRST_NAME_STRING, operation.firstName() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
