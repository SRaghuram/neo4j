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

package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTrianglesResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery17;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery17EmbeddedCypher extends Neo4jSnbBiQuery17<Neo4jConnectionState>
{
    @Override
    public LdbcSnbBiQuery17FriendshipTrianglesResult execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery17FriendshipTriangles operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery17EmbeddedCypher::transformResult );
    }

    private static LdbcSnbBiQuery17FriendshipTrianglesResult transformResult( Result result )
    {
        Map<String,Object> row = result.next();
        return new LdbcSnbBiQuery17FriendshipTrianglesResult(
                ((Number) row.get( "count(*)" )).intValue() );
    }
}
