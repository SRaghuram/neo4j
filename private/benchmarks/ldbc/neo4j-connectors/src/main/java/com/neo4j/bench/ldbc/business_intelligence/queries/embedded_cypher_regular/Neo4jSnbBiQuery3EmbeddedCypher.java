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
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolution;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolutionResult;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery3;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

public class Neo4jSnbBiQuery3EmbeddedCypher extends Neo4jSnbBiQuery3<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery3TagEvolutionResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery3TagEvolution operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery3EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery3TagEvolutionResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery3TagEvolutionResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            transformedResult.add(
                    new LdbcSnbBiQuery3TagEvolutionResult(
                            (String) row.get( "tagName" ),
                            ((Number) row.get( "countMonth1" )).intValue(),
                            ((Number) row.get( "countMonth2" )).intValue(),
                            ((Number) row.get( "diff" )).intValue() )
            );
        }
        return transformedResult;
    }
}
