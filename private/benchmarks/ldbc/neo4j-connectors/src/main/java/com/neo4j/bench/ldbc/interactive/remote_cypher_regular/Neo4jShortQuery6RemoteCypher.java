/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery6;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static java.lang.String.format;

public class Neo4jShortQuery6RemoteCypher extends Neo4jShortQuery6<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public LdbcShortQuery6MessageForumResult execute( Neo4jConnectionState connection,
            LdbcShortQuery6MessageForum operation ) throws DbException
    {
        try ( Session session = connection.session() )
        {
            Result statementResult = session.run(
                    connection.queries().queryFor( operation ).queryString(),
                    buildParams( operation )
            );
            Record record = statementResult.next();
            return new LdbcShortQuery6MessageForumResult(
                    record.get( "forumId" ).asLong(),
                    record.get( "forumTitle" ).asString(),
                    record.get( "moderatorId" ).asLong(),
                    record.get( "moderatorFirstName" ).asString(),
                    record.get( "moderatorLastName" ).asString()
            );
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Error Executing: %s", operation ), e );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery6MessageForum operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
