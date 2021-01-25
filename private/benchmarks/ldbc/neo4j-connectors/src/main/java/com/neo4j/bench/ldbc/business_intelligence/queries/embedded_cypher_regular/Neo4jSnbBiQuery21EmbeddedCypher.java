/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21Zombies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21ZombiesResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery21;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery21EmbeddedCypher extends Neo4jSnbBiQuery21<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery21ZombiesResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery21Zombies operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery21EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery21ZombiesResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery21ZombiesResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery21ZombiesResult(
                            ((Number) row.get( "zombie.id" )).longValue(),
                            ((Number) row.get( "zombieLikeCount" )).intValue(),
                            ((Number) row.get( "totalLikeCount" )).intValue(),
                            ((Number) row.get( "zombieScore" )).doubleValue() )
            );
        }
        return transformedResult;
    }
}
