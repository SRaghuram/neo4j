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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate3;

import java.util.HashMap;
import java.util.Map;

public class Neo4jUpdate3EmbeddedCypher extends Neo4jUpdate3<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String COMMENT_ID_STRING = COMMENT_ID.toString();
    protected static final String CREATION_DATE_STRING = CREATION_DATE.toString();

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate3AddCommentLike operation )
            throws DbException
    {
        connection.db().execute(
                connection.queries().queryFor( operation ).queryString(),
                buildParams( operation, connection.dateUtil() ) );
        return LdbcNoResult.INSTANCE;
    }

    private Map<String,Object> buildParams( LdbcUpdate3AddCommentLike operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( COMMENT_ID_STRING, operation.commentId() );
        queryParams.put( CREATION_DATE_STRING, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        return queryParams;
    }
}
