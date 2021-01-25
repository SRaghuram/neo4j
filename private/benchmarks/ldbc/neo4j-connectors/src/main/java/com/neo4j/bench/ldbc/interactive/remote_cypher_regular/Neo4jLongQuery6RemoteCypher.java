/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery6;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static java.lang.String.format;

public class Neo4jLongQuery6RemoteCypher extends Neo4jQuery6<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String TAG_NAME_STRING = TAG_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery6Result> execute( Neo4jConnectionState connection, LdbcQuery6 operation )
            throws DbException
    {
        List<LdbcQuery6Result> result = new ArrayList<>( operation.limit() );
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
                        new LdbcQuery6Result(
                                record.get( "tagName" ).asString(),
                                record.get( "postCount" ).asInt()
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

private Map<String,Object> buildParams( LdbcQuery6 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( TAG_NAME_STRING, operation.tagName() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
