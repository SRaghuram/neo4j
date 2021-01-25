/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopic;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopicResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery24;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery24EmbeddedCypher extends Neo4jSnbBiQuery24<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery24MessagesByTopicResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery24MessagesByTopic operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery24EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery24MessagesByTopicResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery24MessagesByTopicResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery24MessagesByTopicResult(
                            ((Number) row.get( "messsageCount" )).intValue(),
                            ((Number) row.get( "likeCount" )).intValue(),
                            ((Number) row.get( "year" )).intValue(),
                            ((Number) row.get( "month" )).intValue(),
                            (String) row.get( "continent.name" ) )
            );
        }
        return transformedResult;
    }
}
