/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedReplies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedRepliesResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery11;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery11EmbeddedCypher extends Neo4jSnbBiQuery11<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery11UnrelatedRepliesResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery11UnrelatedReplies operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery11EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery11UnrelatedRepliesResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery11UnrelatedRepliesResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery11UnrelatedRepliesResult(
                            ((Number) row.get( "person.id" )).longValue(),
                            (String) row.get( "tag.name" ),
                            ((Number) row.get( "countLikes" )).intValue(),
                            ((Number) row.get( "countReplies" )).intValue() )
            );
        }
        return transformedResult;
    }
}
