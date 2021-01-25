/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopicsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery20;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery20EmbeddedCypher extends Neo4jSnbBiQuery20<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery20HighLevelTopicsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery20HighLevelTopics operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery20EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery20HighLevelTopicsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery20HighLevelTopicsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery20HighLevelTopicsResult(
                            (String) row.get( "tagClass.name" ),
                            ((Number) row.get( "postCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
