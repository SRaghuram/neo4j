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
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.Neo4jSnbBiQuery13;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static java.util.stream.Collectors.toList;

public class Neo4jSnbBiQuery13EmbeddedCypher extends Neo4jSnbBiQuery13<Neo4jConnectionState>
{
    @Override
    public List<LdbcSnbBiQuery13PopularMonthlyTagsResult> execute(
            Neo4jConnectionState connection,
            LdbcSnbBiQuery13PopularMonthlyTags operation ) throws DbException
    {
        return SnbBiEmbeddedCypherRegularCommands.execute(
                connection,
                operation,
                Neo4jSnbBiQuery13EmbeddedCypher::transformResult );
    }

    private static List<LdbcSnbBiQuery13PopularMonthlyTagsResult> transformResult( Result result )
    {
        List<LdbcSnbBiQuery13PopularMonthlyTagsResult> transformedResult = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            List<TagPopularity> topPopularTags = ((List<List<Object>>) row.get( "topPopularTags" )).stream()
                    .map( l -> new TagPopularity(
                            (String) l.get( 0 ),
                            ((Number) l.get( 1 )).intValue() ) )
                    .collect( toList() );
            transformedResult.add(
                    new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                            ((Number) row.get( "year" )).intValue(),
                            ((Number) row.get( "month" )).intValue(),
                            topPopularTags )
            );
        }
        return transformedResult;
    }
}
