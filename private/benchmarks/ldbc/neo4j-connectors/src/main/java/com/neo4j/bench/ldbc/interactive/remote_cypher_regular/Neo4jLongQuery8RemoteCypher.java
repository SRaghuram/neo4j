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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static java.lang.String.format;

public class Neo4jLongQuery8RemoteCypher extends Neo4jQuery8<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery8Result> execute( Neo4jConnectionState connection, LdbcQuery8 operation )
            throws DbException
    {
        List<LdbcQuery8Result> result = new ArrayList<>( operation.limit() );
        QueryDateUtil dateUtil = connection.dateUtil();
        try ( Session session = connection.session() )
        {
            StatementResult statementResult = session.run(
                    connection.queries().queryFor( operation ).queryString(),
                    buildParams( operation )
            );
            while ( statementResult.hasNext() )
            {
                Record record = statementResult.next();
                result.add(
                        new LdbcQuery8Result(
                                record.get( "personId" ).asLong(),
                                record.get( "personFirstName" ).asString(),
                                record.get( "personLastName" ).asString(),
                                dateUtil.formatToUtc( record.get( "commentCreationDate" ).asLong() ),
                                record.get( "commentId" ).asLong(),
                                record.get( "commentContent" ).asString()
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

    private Map<String,Object> buildParams( LdbcQuery8 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
