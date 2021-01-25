/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate5;

import java.util.HashMap;
import java.util.Map;

public class Neo4jUpdate5EmbeddedCypher extends Neo4jUpdate5<Neo4jConnectionState>
{
    protected static final String FORUM_ID_STRING = FORUM_ID.toString();
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String CREATION_DATE_STRING = CREATION_DATE.toString();

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate5AddForumMembership operation )
            throws DbException
    {
        connection.execute(
                connection.queries().queryFor( operation ).queryString(),
                buildParams( operation, connection.dateUtil() ) );
        return LdbcNoResult.INSTANCE;
    }

    private Map<String,Object> buildParams( LdbcUpdate5AddForumMembership operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( FORUM_ID_STRING, operation.forumId() );
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( CREATION_DATE_STRING, dateUtil.utcToFormat( operation.joinDate().getTime() ) );
        return queryParams;
    }
}
