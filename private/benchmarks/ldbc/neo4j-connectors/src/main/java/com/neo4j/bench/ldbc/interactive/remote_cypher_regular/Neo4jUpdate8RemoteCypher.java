/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate8;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Session;

import static java.lang.String.format;

public class Neo4jUpdate8RemoteCypher extends Neo4jUpdate8<Neo4jConnectionState>
{
    protected static final String PERSON_1_ID_STRING = PERSON_1_ID.toString();
    protected static final String PERSON_2_ID_STRING = PERSON_2_ID.toString();
    protected static final String FRIENDSHIP_CREATION_DATE_STRING = FRIENDSHIP_CREATION_DATE.toString();

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate8AddFriendship operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        try ( Session session = connection.session() )
        {
            session.run(
                    connection.queries().queryFor( operation ).queryString(),
                    buildParams( operation, dateUtil )
            );
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Error Executing: %s", operation ), e );
        }
        return LdbcNoResult.INSTANCE;
    }

    private Map<String,Object> buildParams( LdbcUpdate8AddFriendship operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_1_ID_STRING, operation.person1Id() );
        queryParams.put( PERSON_2_ID_STRING, operation.person2Id() );
        queryParams.put( FRIENDSHIP_CREATION_DATE_STRING, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        return queryParams;
    }
}
