/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery7;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static java.lang.String.format;

public class Neo4jLongQuery7RemoteCypher extends Neo4jQuery7<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery7Result> execute( Neo4jConnectionState connection, LdbcQuery7 operation )
            throws DbException
    {
        List<LdbcQuery7Result> result = new ArrayList<>( operation.limit() );
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
                long messageCreationDateAsUtc = dateUtil.formatToUtc( record.get( "messageCreationDate" ).asLong() );
                long likeTimeAsUtc = dateUtil.formatToUtc( record.get( "likeTime" ).asLong() );
                long latencyAsMilli = likeTimeAsUtc - messageCreationDateAsUtc;
                Long latencyAsMinutes = (latencyAsMilli / 1000) / 60;
                result.add(
                        new LdbcQuery7Result(
                                record.get( "personId" ).asLong(),
                                record.get( "personFirstName" ).asString(),
                                record.get( "personLastName" ).asString(),
                                likeTimeAsUtc,
                                record.get( "messageId" ).asLong(),
                                record.get( "messageContent" ).asString(),
                                latencyAsMinutes.intValue(),
                                record.get( "isNew" ).asBoolean()
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

    private Map<String,Object> buildParams( LdbcQuery7 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
