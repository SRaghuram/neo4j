/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsers;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsersResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery7;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery7EmbeddedCypher extends Neo4jSnbBiQuery7<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery7AuthoritativeUsersResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery7AuthoritativeUsers operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery7EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery7AuthoritativeUsersResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery7AuthoritativeUsersResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery7AuthoritativeUsersResult(
                            ((Number) row.get( "person1.id" )).longValue(),
                            ((Number) row.get( "authorityScore" )).intValue() )
            );
        }
        return transformedResult;
    }
}
