/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinations;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinationsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery23;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery23EmbeddedCypher extends Neo4jSnbBiQuery23<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery23HolidayDestinationsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery23HolidayDestinations operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery23EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery23HolidayDestinationsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery23HolidayDestinationsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery23HolidayDestinationsResult(
                            ((Number) row.get( "messsageCount" )).intValue(),
                            (String) row.get( "destination.name" ),
                            ((Number) row.get( "month" )).intValue() )
            );
        }
        return transformedResult;
    }
}
