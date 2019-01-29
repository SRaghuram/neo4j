/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummaryResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery1;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary.DATE;

public class Neo4jSnbBiQuery1EmbeddedCypher extends Neo4jSnbBiQuery1<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery1PostingSummaryResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery1PostingSummary operation ) throws DbException
    {
        // TODO remove this double map instantiation
        Map<String,Object> params = new HashMap<>( operation.parameterMap() );
        params.put(
                DATE,
                LdbcDateCodec.utcToEncodedLongDateTime( operation.date(), connection.calendar() ) );

        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery1EmbeddedCypher::transformResult,
                params );
    }

    private static List<LdbcSnbBiQuery1PostingSummaryResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery1PostingSummaryResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery1PostingSummaryResult(
                            ((Number) row.get( "year" )).intValue(),
                            (boolean) row.get( "isComment" ),
                            ((Number) row.get( "lengthCategory" )).intValue(),
                            ((Number) row.get( "messageCount" )).longValue(),
                            ((Number) row.get( "averageMessageLength" )).longValue(),
                            ((Number) row.get( "sumMessageLength" )).longValue(),
                            ((Number) row.get( "percentageOfMessages" )).floatValue()
                    )
            );
        }
        return transformedResult;
    }
}
