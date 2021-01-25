/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery7;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static java.lang.String.format;

public class Neo4jShortQuery7RemoteCypher extends Neo4jShortQuery7<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery7MessageReplies operation ) throws DbException
    {
        List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();
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
                        new LdbcShortQuery7MessageRepliesResult(
                                record.get( "replyId" ).asLong(),
                                record.get( "replyContent" ).asString(),
                                dateUtil.formatToUtc( record.get( "replyCreationDate" ).asLong() ),
                                record.get( "replyAuthorId" ).asLong(),
                                record.get( "replyAuthorFirstName" ).asString(),
                                record.get( "replyAuthorLastName" ).asString(),
                                record.get( "replyAuthorKnowsAuthor" ).asBoolean()
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

    private Map<String,Object> buildParams( LdbcShortQuery7MessageReplies operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
