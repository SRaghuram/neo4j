/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTagsResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery2;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags.END_DATE;
import static com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags.START_DATE;

public class Neo4jSnbBiQuery2EmbeddedCypher extends Neo4jSnbBiQuery2<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery2TopTagsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery2TopTags operation ) throws DbException
    {
        // TODO remove this double map instantiation
        Map<String,Object> params = new HashMap<>( operation.parameterMap() );
        params.put(
                START_DATE,
                connection.dateUtil().ldbcDateCodecUtil().utcToEncodedLongDateTime(
                        operation.startDate(),
                        connection.calendar() ) );
        params.put(
                END_DATE,
                connection.dateUtil().ldbcDateCodecUtil().utcToEncodedLongDateTime(
                        operation.endDate(),
                        connection.calendar() ) );

        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery2EmbeddedCypher::transformResult,
                params );
    }

    private static List<LdbcSnbBiQuery2TopTagsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery2TopTagsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery2TopTagsResult(
                            (String) row.get( "countryName" ),
                            ((Number) row.get( "messageMonth" )).intValue(),
                            (String) row.get( "personGender" ),
                            ((Number) row.get( "ageGroup" )).intValue(),
                            (String) row.get( "tagName" ),
                            ((Number) row.get( "messageCount" )).intValue() )
            );
        }
        return transformedResult;
    }
}
