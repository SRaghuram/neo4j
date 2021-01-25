/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCounts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCountsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery18;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery18EmbeddedCypher extends Neo4jSnbBiQuery18<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery18PersonPostCountsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery18PersonPostCounts operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery18EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery18PersonPostCountsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery18PersonPostCountsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery18PersonPostCountsResult(
                            ((Number) row.get( "messageCount" )).intValue(),
                            ((Number) row.get( "personCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
