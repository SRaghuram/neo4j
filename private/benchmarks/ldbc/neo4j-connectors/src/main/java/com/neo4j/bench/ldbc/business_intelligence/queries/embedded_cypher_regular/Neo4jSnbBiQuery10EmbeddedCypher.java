/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPerson;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPersonResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery10;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery10EmbeddedCypher extends Neo4jSnbBiQuery10<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery10TagPersonResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery10TagPerson operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery10EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery10TagPersonResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery10TagPersonResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery10TagPersonResult(
                            ((Number) row.get( "person.id" )).longValue(),
                            ((Number) row.get( "score" )).intValue(),
                            ((Number) row.get( "friendsScore" )).intValue() )
            );
        }
        return transformedResult;
    }
}
