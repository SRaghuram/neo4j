/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePostersResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery6;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery6EmbeddedCypher extends Neo4jSnbBiQuery6<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery6ActivePostersResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery6ActivePosters operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery6EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery6ActivePostersResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery6ActivePostersResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery6ActivePostersResult(
                            ((Number) row.get( "person.id" )).longValue(),
                            ((Number) row.get( "replyCount" )).intValue(),
                            ((Number) row.get( "likeCount" )).intValue(),
                            ((Number) row.get( "messageCount" )).intValue(),
                            ((Number) row.get( "score" )).intValue() )
            );
        }
        return transformedResult;
    }
}
