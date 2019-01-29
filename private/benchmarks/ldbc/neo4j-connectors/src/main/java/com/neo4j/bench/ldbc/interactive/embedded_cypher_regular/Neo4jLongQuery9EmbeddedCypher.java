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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery9;
import com.neo4j.bench.ldbc.utils.PlanMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

public class Neo4jLongQuery9EmbeddedCypher extends Neo4jQuery9<Neo4jConnectionState>
{
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String LATEST_DATE_STRING = LATEST_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public List<LdbcQuery9Result> execute( Neo4jConnectionState connection, LdbcQuery9 operation )
            throws DbException
    {
        if ( connection.isFirstForType( operation.type() ) )
        {
            Result defaultPlannerResult = connection.db().execute(
                    withExplain( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation, connection.dateUtil() ) );
            Result executionResult = connection.db().execute(
                    withProfile( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation, connection.dateUtil() ) );
            List<LdbcQuery9Result> results =
                    ImmutableList.copyOf( Iterators.transform(
                            executionResult,
                            new TransformFun( connection.dateUtil() ) ) );
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
                            buildParams( operation, connection.dateUtil() ) ),
                    new TransformFun( connection.dateUtil() ) ) );
        }
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcQuery9Result>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcQuery9Result apply( Map<String,Object> row )
        {
            return new LdbcQuery9Result(
                    (long) row.get( "personId" ),
                    (String) row.get( "personFirstName" ),
                    (String) row.get( "personLastName" ),
                    (long) row.get( "messageId" ),
                    (String) row.get( "messageContent" ),
                    dateUtil.formatToUtc( (long) row.get( "messageCreationDate" ) ) );
        }
    }

    private Map<String,Object> buildParams( LdbcQuery9 operation, QueryDateUtil dateUtil )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( PERSON_ID_STRING, operation.personId() );
        queryParams.put( LATEST_DATE_STRING, dateUtil.utcToFormat( operation.maxDate().getTime() ) );
        queryParams.put( LIMIT_STRING, operation.limit() );
        return queryParams;
    }
}
