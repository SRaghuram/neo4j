/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery10;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static java.lang.String.format;

public class Neo4jLongQuery10RemoteCypher extends Neo4jQuery10<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String MONTH_STRING = MONTH.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery10Result> execute( Neo4jConnectionState connection, LdbcQuery10 operation )
            throws DbException
    {
        List<LdbcQuery10Result> result = new ArrayList<>( operation.limit() );
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
                        new LdbcQuery10Result(
                                record.get( "personId" ).asLong(),
                                record.get( "personFirstName" ).asString(),
                                record.get( "personLastName" ).asString(),
                                record.get( "commonInterestScore" ).asInt(),
                                record.get( "personGender" ).asString(),
                                record.get( "personCityName" ).asString()
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

private Map<String,Object> buildParams( LdbcQuery10 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( MONTH_STRING, operation.month() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
