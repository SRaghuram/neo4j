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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery6;
import com.neo4j.bench.ldbc.utils.PlanMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

public class Neo4jLongQuery6EmbeddedCypher extends Neo4jQuery6<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String TAG_NAME_STRING = TAG_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery6Result> execute( Neo4jConnectionState connection, LdbcQuery6 operation )
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
            List<LdbcQuery6Result> results =
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
            return Lists.newArrayList( Iterators.transform(
                    connection.db().execute(
                            connection.queries().queryFor( operation ).queryString(),
                            buildParams( operation ) ),
                    TRANSFORM_FUN ) );
        }
    }

    private static final Function<Map<String,Object>,LdbcQuery6Result> TRANSFORM_FUN =
            new Function<Map<String,Object>,LdbcQuery6Result>()
            {
                @Override
                public LdbcQuery6Result apply( Map<String,Object> row )
                {
                    return new LdbcQuery6Result(
                            (String) row.get( "tagName" ),
                            ((Long) row.get( "postCount" )).intValue() );
                }
            };

    private Map<String,Object> buildParams( LdbcQuery6 operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( TAG_NAME_STRING, operation.tagName() );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
