/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
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
