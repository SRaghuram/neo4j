/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormals;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormalsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery15;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery15EmbeddedCypher extends Neo4jSnbBiQuery15<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery15SocialNormalsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery15SocialNormals operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery15EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery15SocialNormalsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery15SocialNormalsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery15SocialNormalsResult(
                            ((Number) row.get( "person2.id" )).longValue(),
                            ((Number) row.get( "count" )).intValue() )
            );
        }
        return transformedResult;
    }
}
