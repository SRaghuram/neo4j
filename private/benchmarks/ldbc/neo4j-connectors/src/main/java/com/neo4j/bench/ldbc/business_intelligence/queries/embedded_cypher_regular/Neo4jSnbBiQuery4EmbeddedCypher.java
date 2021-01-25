/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopicsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery4;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery4EmbeddedCypher extends Neo4jSnbBiQuery4<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery4PopularCountryTopicsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery4PopularCountryTopics operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery4EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery4PopularCountryTopicsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery4PopularCountryTopicsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery4PopularCountryTopicsResult(
                            ((Number) row.get( "forum.id" )).longValue(),
                            (String) row.get( "forum.title" ),
                            ((Number) row.get( "forum.creationDate" )).longValue(),
                            ((Number) row.get( "person.id" )).longValue(),
                            ((Number) row.get( "postCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
