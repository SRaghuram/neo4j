/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialog;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialogResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery22;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery22EmbeddedCypher extends Neo4jSnbBiQuery22<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery22InternationalDialogResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery22InternationalDialog operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery22EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery22InternationalDialogResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery22InternationalDialogResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery22InternationalDialogResult(
                            ((Number) row.get( "top.person1.id" )).longValue(),
                            ((Number) row.get( "top.person2.id" )).longValue(),
                            (String) row.get( "city1.name" ),
                            ((Number) row.get( "top.score" )).intValue() )
            );
        }
        return transformedResult;
    }
}
