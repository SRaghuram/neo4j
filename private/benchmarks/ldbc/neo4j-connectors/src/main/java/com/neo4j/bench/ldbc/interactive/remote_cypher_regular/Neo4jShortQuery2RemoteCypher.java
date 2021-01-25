/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static java.lang.String.format;

public class Neo4jShortQuery2RemoteCypher extends Neo4jShortQuery2<Neo4jConnectionState>
{
    private static final String PERSON_ID_STRING = PERSON_ID.toString();
    private static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcShortQuery2PersonPostsResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery2PersonPosts operation ) throws DbException
    {
        List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>( operation.limit() );
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
                        new LdbcShortQuery2PersonPostsResult(
                                record.get( "messageId" ).asLong(),
                                record.get( "messageContent" ).asString(),
                                dateUtil.formatToUtc( record.get( "messageCreationDate" ).asLong() ),
                                record.get( "postId" ).asLong(),
                                record.get( "personId" ).asLong(),
                                record.get( "personFirstName" ).asString(),
                                record.get( "personLastName" ).asString()
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

    private Map<String,Object> buildParams( LdbcShortQuery2PersonPosts operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
