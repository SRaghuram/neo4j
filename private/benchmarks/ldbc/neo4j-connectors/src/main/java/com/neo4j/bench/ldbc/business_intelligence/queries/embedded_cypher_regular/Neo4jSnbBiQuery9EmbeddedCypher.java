/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForums;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForumsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery9;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery9EmbeddedCypher extends Neo4jSnbBiQuery9<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery9RelatedForumsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery9RelatedForums operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery9EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery9RelatedForumsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery9RelatedForumsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery9RelatedForumsResult(
                            ((Number) row.get( "forum.id" )).longValue(),
                            ((Number) row.get( "count1" )).intValue(),
                            ((Number) row.get( "count2" )).intValue() )
            );
        }
        return transformedResult;
    }
}
