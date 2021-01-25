/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiators;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiatorsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery14;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery14EmbeddedCypher extends Neo4jSnbBiQuery14<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery14TopThreadInitiatorsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery14TopThreadInitiators operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery14EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery14TopThreadInitiatorsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery14TopThreadInitiatorsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                            ((Number) row.get( "person.id" )).longValue(),
                            (String) row.get( "person.firstName" ),
                            (String) row.get( "person.lastName" ),
                            ((Number) row.get( "threadCount" )).intValue(),
                            ((Number) row.get( "messageCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
