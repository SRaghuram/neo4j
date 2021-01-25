/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPostersResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery5;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery5EmbeddedCypher extends Neo4jSnbBiQuery5<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery5TopCountryPostersResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery5TopCountryPosters operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery5EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery5TopCountryPostersResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery5TopCountryPostersResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery5TopCountryPostersResult(
                            ((Number) row.get( "person.id" )).longValue(),
                            (String) row.get( "person.firstName" ),
                            (String) row.get( "person.lastName" ),
                            ((Number) row.get( "person.creationDate" )).longValue(),
                            ((Number) row.get( "postCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
