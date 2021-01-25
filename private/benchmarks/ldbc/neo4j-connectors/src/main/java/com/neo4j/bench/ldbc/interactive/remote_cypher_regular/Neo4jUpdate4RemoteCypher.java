/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate4;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Session;

import static java.lang.String.format;

public class Neo4jUpdate4RemoteCypher extends Neo4jUpdate4<Neo4jConnectionState>
{
    protected static final String FORUM_PARAMS_STRING = FORUM_PARAMS.toString();
    protected static final String MODERATOR_PERSON_ID_STRING = MODERATOR_PERSON_ID.toString();
    protected static final String TAG_IDS_STRING = TAG_IDS.toString();

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate4AddForum operation )
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

    private Map<String,Object> buildParams( LdbcUpdate4AddForum operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> forumParams = new HashMap<>();
        forumParams.put( Forum.ID, operation.forumId() );
        forumParams.put( Forum.TITLE, operation.forumTitle() );
        forumParams.put( Forum.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );

        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( FORUM_PARAMS_STRING, forumParams );
        queryParams.put( MODERATOR_PERSON_ID_STRING, operation.moderatorPersonId() );
        queryParams.put( TAG_IDS_STRING, operation.tagIds() );
        return queryParams;
    }
}
