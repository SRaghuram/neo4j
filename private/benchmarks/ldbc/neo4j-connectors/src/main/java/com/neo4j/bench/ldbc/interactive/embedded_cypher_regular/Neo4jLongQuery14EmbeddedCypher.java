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

package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery14;
import com.neo4j.bench.ldbc.utils.PlanMeta;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

public class Neo4jLongQuery14EmbeddedCypher extends Neo4jQuery14<Neo4jConnectionState>
{
    protected static final String PERSON_ID_1_STRING = PERSON_ID_1.toString();
    protected static final String PERSON_ID_2_STRING = PERSON_ID_2.toString();

    @Override
    public List<LdbcQuery14Result> execute( Neo4jConnectionState connection, LdbcQuery14 operation )
            throws DbException
    {
        if ( connection.isFirstForType( operation.type() ) )
        {
            Result defaultPlannerResult = connection.db().execute(
                    withExplain( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            Result executionResult = connection.db().execute(
                    withProfile( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            List<LdbcQuery14Result> results =
                    ImmutableList.copyOf( Iterators.transform( executionResult, TRANSFORM_FUN ) );
            // force materialize
            results.size();
            connection.reportPlanStats(
                    operation,
                    PlanMeta.extractPlanner( defaultPlannerResult.getExecutionPlanDescription() ),
                    PlanMeta.extractPlanner( executionResult.getExecutionPlanDescription() ),
                    executionResult.getExecutionPlanDescription()
            );
            return results;
        }
        else
        {
            return Lists.newArrayList(
                    Iterators.transform(
                            connection.db().execute(
                                    connection.queries().queryFor( operation ).queryString(),
                                    buildParams( operation ) ),
                            TRANSFORM_FUN
                    )
            );
        }
    }

    private static final Function<Map<String,Object>,LdbcQuery14Result> TRANSFORM_FUN =
            new Function<Map<String,Object>,LdbcQuery14Result>()
            {
                @Override
                public LdbcQuery14Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery14Result(
                            (Collection<Long>) row.get( "pathNodeIds" ),
                            (double) row.get( "weight" ) );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery14 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_1_STRING, operation.person1Id() );
        queryParams.put( PERSON_ID_2_STRING, operation.person2Id() );
        return queryParams;
    }
}
