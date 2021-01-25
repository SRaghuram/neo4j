/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPosts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPostsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery12;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery12EmbeddedCypher extends Neo4jSnbBiQuery12<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery12TrendingPostsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery12TrendingPosts operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery12EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery12TrendingPostsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery12TrendingPostsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery12TrendingPostsResult(
                            ((Number) row.get( "message.id" )).longValue(),
                            ((Number) row.get( "message.creationDate" )).longValue(),
                            (String) row.get( "creator.firstName" ),
                            (String) row.get( "creator.lastName" ),
                            ((Number) row.get( "likeCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
