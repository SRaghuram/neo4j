/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate4;

import java.util.HashMap;
import java.util.Map;

public class Neo4jUpdate4EmbeddedCypher extends Neo4jUpdate4<Neo4jConnectionState>
{
    protected static final String FORUM_PARAMS_STRING = FORUM_PARAMS.toString();
    protected static final String MODERATOR_PERSON_ID_STRING = MODERATOR_PERSON_ID.toString();
    protected static final String TAG_IDS_STRING = TAG_IDS.toString();

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate4AddForum operation )
            throws DbException
    {
        connection.db().execute(
                connection.queries().queryFor( operation ).queryString(),
                buildParams( operation, connection.dateUtil() ) );
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
