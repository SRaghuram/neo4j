/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteraction;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteractionResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery19;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery19EmbeddedCypher extends Neo4jSnbBiQuery19<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery19StrangerInteractionResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery19StrangerInteraction operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery19EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery19StrangerInteractionResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery19StrangerInteractionResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery19StrangerInteractionResult(
                            ((Number) row.get( "person.id" )).longValue(),
                            ((Number) row.get( "strangersCount" )).intValue(),
                            ((Number) row.get( "interactionCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
