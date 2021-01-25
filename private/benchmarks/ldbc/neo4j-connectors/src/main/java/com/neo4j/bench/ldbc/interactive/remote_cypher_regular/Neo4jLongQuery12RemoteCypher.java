/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery12;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import java.util.function.Function;

import static java.lang.String.format;

public class Neo4jLongQuery12RemoteCypher extends Neo4jQuery12<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String TAG_CLASS_NAME_STRING = TAG_CLASS_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery12Result> execute( Neo4jConnectionState connection, LdbcQuery12 operation )
            throws DbException
    {
        List<LdbcQuery12Result> result = new ArrayList<>( operation.limit() );
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
                        new LdbcQuery12Result(
                                record.get( "friendId" ).asLong(),
                                record.get( "friendFirstName" ).asString(),
                                record.get( "friendLastName" ).asString(),
                                record.get( "tagNames" ).asList( LIST_VALUE_TO_STRING_LIST_FUN ),
                                record.get( "count" ).asInt()
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

    private Map<String,Object> buildParams( LdbcQuery12 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( TAG_CLASS_NAME_STRING, operation.tagClassName() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
