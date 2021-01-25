/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery25WeightedPaths;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery25WeightedPathsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery25;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery25EmbeddedCypher extends Neo4jSnbBiQuery25<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery25WeightedPathsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery25WeightedPaths operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery25EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery25WeightedPathsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery25WeightedPathsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery25WeightedPathsResult(
                            (List<Long>) row.get( "personIds" ),
                            ((Number) row.get( "weight" )).doubleValue() )
            );
        }
        return transformedResult;
    }
}
