/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static java.lang.String.format;

public class Neo4jLongQuery3RemoteCypher extends Neo4jQuery3<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String COUNTRY_X_STRING = COUNTRY_X.toString();
    protected static final String COUNTRY_Y_STRING = COUNTRY_Y.toString();
    protected static final String MIN_DATE_STRING = MIN_DATE.toString();
    protected static final String MAX_DATE_STRING = MAX_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery3Result> execute( Neo4jConnectionState connection, LdbcQuery3 operation )
            throws DbException
    {
        List<LdbcQuery3Result> result = new ArrayList<>( operation.limit() );
        QueryDateUtil dateUtil = connection.dateUtil();
        try ( Session session = connection.session() )
        {
            Result statementResult = session.run(
                    connection.queries().queryFor( operation ).queryString(),
                    buildParams( operation, dateUtil )
            );
            while ( statementResult.hasNext() )
            {
                Record record = statementResult.next();
                result.add(
                        new LdbcQuery3Result(
                                record.get( "friendId" ).asLong(),
                                record.get( "friendFirstName" ).asString(),
                                record.get( "friendLastName" ).asString(),
                                record.get( "xCount" ).asInt(),
                                record.get( "yCount" ).asInt(),
                                record.get( "xyCount" ).asInt()
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

    private Map<String,Object> buildParams( LdbcQuery3 operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( COUNTRY_X_STRING, operation.countryXName() );
        queryParams.put( COUNTRY_Y_STRING, operation.countryYName() );
        long startDateAsMilli = operation.startDate().getTime();
        int durationHours = operation.durationDays() * 24;
        long endDateAsMilli = startDateAsMilli + TimeUnit.HOURS.toMillis( durationHours );
        queryParams.put( MIN_DATE_STRING, dateUtil.utcToFormat( startDateAsMilli ) );
        queryParams.put( MAX_DATE_STRING, dateUtil.utcToFormat( endDateAsMilli ) );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
