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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery13;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static java.lang.String.format;

public class Neo4jLongQuery13RemoteCypher extends Neo4jQuery13<Neo4jConnectionState>
{
    protected static final String PERSON_ID_1_STRING = PERSON_ID_1.toString();
    protected static final String PERSON_ID_2_STRING = PERSON_ID_2.toString();

    @Override
    public LdbcQuery13Result execute( Neo4jConnectionState connection, LdbcQuery13 operation ) throws DbException
    {
        try ( Session session = connection.session() )
        {
            StatementResult statementResult = session.run(
                    connection.queries().queryFor( operation ).queryString(),
                    buildParams( operation )
            );
            return new LdbcQuery13Result( statementResult.next().get( "pathLength" ).asInt() );
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Error Executing: %s", operation ), e );
        }
    }

    private Map<String,Object> buildParams( LdbcQuery13 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_1_STRING, operation.person1Id() );
        queryParams.put( PERSON_ID_2_STRING, operation.person2Id() );
        return queryParams;
    }
}
