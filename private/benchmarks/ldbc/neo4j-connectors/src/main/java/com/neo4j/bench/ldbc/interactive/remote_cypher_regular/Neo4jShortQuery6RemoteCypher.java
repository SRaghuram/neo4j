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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery6;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static java.lang.String.format;

public class Neo4jShortQuery6RemoteCypher extends Neo4jShortQuery6<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public LdbcShortQuery6MessageForumResult execute( Neo4jConnectionState connection,
            LdbcShortQuery6MessageForum operation ) throws DbException
    {
        try ( Session session = connection.session() )
        {
            StatementResult statementResult = session.run(
                    connection.queries().queryFor( operation ).queryString(),
                    buildParams( operation )
            );
            Record record = statementResult.next();
            return new LdbcShortQuery6MessageForumResult(
                    record.get( "forumId" ).asLong(),
                    record.get( "forumTitle" ).asString(),
                    record.get( "moderatorId" ).asLong(),
                    record.get( "moderatorFirstName" ).asString(),
                    record.get( "moderatorLastName" ).asString()
            );
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Error Executing: %s", operation ), e );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery6MessageForum operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
