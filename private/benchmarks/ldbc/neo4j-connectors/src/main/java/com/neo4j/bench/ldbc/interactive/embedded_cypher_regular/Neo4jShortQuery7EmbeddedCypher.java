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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery7;
import com.neo4j.bench.ldbc.utils.PlanMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Result;

import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withExplain;
import static com.neo4j.bench.ldbc.utils.AnnotatedQuery.withProfile;

public class Neo4jShortQuery7EmbeddedCypher extends Neo4jShortQuery7<Neo4jConnectionState>
{
    private static final String MESSAGE_ID_STRING = MESSAGE_ID.toString();

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery7MessageReplies operation ) throws DbException
    {
        if ( connection.isFirstForType( operation.type() ) )
        {
            Result defaultPlannerResult = connection.db().execute(
                    withExplain( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            Result executionResult = connection.db().execute(
                    withProfile( connection.queries().queryFor( operation ).queryString() ),
                    buildParams( operation ) );
            List<LdbcShortQuery7MessageRepliesResult> results =
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
                            buildParams( operation ) ),
                    new TransformFun( connection.dateUtil() ) ) );
        }
    }

    private static class TransformFun implements Function<Map<String,Object>,LdbcShortQuery7MessageRepliesResult>
    {
        private final QueryDateUtil dateUtil;

        TransformFun( QueryDateUtil dateUtil )
        {
            this.dateUtil = dateUtil;
        }

        @Override
        public LdbcShortQuery7MessageRepliesResult apply( Map<String,Object> row )
        {
            return new LdbcShortQuery7MessageRepliesResult(
                    (long) row.get( "replyId" ),
                    (String) row.get( "replyContent" ),
                    dateUtil.formatToUtc( (long) row.get( "replyCreationDate" ) ),
                    (long) row.get( "replyAuthorId" ),
                    (String) row.get( "replyAuthorFirstName" ),
                    (String) row.get( "replyAuthorLastName" ),
                    (boolean) row.get( "replyAuthorKnowsAuthor" )
            );
        }
    }

    private Map<String,Object> buildParams( LdbcShortQuery7MessageReplies operation )
    {
        Map<String,Object> queryParams = new HashMap<>();
        queryParams.put( MESSAGE_ID_STRING, operation.messageId() );
        return queryParams;
    }
}
