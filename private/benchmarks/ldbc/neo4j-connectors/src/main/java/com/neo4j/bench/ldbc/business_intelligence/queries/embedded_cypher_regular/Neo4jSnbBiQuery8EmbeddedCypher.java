/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopicsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery8;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery8EmbeddedCypher extends Neo4jSnbBiQuery8<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery8RelatedTopicsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery8RelatedTopics operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery8EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery8RelatedTopicsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery8RelatedTopicsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery8RelatedTopicsResult(
                            (String) row.get( "relatedTag.name" ),
                            ((Number) row.get( "count" )).intValue() )
            );
        }
        return transformedResult;
    }
}
