/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircleResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery16;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery16EmbeddedCypher extends Neo4jSnbBiQuery16<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery16ExpertsInSocialCircle operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery16EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                            ((Number) row.get( "person.id" )).longValue(),
                            (String) row.get( "tag.name" ),
                            ((Number) row.get( "messageCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
