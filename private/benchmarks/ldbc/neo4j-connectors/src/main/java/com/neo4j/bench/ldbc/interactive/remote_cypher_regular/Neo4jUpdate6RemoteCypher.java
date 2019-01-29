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

package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate6;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Session;

import static java.lang.String.format;

public class Neo4jUpdate6RemoteCypher extends Neo4jUpdate6<Neo4jConnectionState>
{
    protected static final String POST_PARAMS_STRING = POST_PARAMS.toString();
    protected static final String AUTHOR_PERSON_ID_STRING = AUTHOR_PERSON_ID.toString();
    protected static final String FORUM_ID_STRING = FORUM_ID.toString();
    protected static final String COUNTRY_ID_STRING = COUNTRY_ID.toString();
    protected static final String TAG_IDS_STRING = TAG_IDS.toString();

    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate6AddPost operation ) throws DbException
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

    private Map<String,Object> buildParams( LdbcUpdate6AddPost operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> postParams = new HashMap<>();
        postParams.put( Message.ID, operation.postId() );
        if ( !operation.imageFile().isEmpty() )
        {
            postParams.put( Post.IMAGE_FILE, operation.imageFile() );
        }
        postParams.put( Message.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        postParams.put( Message.LOCATION_IP, operation.locationIp() );
        postParams.put( Message.BROWSER_USED, operation.browserUsed() );
        postParams.put( Post.LANGUAGE, operation.language() );
        if ( !operation.content().isEmpty() )
        {
            postParams.put( Message.CONTENT, operation.content() );
        }
        postParams.put( Message.LENGTH, operation.content().length() );

        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( POST_PARAMS_STRING, postParams );
        queryParams.put( AUTHOR_PERSON_ID_STRING, operation.authorPersonId() );
        queryParams.put( FORUM_ID_STRING, operation.forumId() );
        queryParams.put( COUNTRY_ID_STRING, operation.countryId() );
        queryParams.put( TAG_IDS_STRING, operation.tagIds() );
        return queryParams;
    }
}
