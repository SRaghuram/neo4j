/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;
import com.neo4j.bench.ldbc.utils.AnnotatedQuery;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import static java.lang.String.format;

public class SnbInteractiveCypherQueries implements AnnotatedQueries
{

    public static SnbInteractiveCypherQueries none()
    {
        return new SnbInteractiveCypherQueries();
    }

    public static SnbInteractiveCypherQueries createWith( Planner plannerType, Runtime runtimeType )
    {
        return new SnbInteractiveCypherQueries()
                .withQuery(
                        LdbcQuery1.TYPE,
                        Neo4jQuery1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery2.TYPE,
                        Neo4jQuery2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery3.TYPE,
                        Neo4jQuery3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery4.TYPE,
                        Neo4jQuery4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery5.TYPE,
                        Neo4jQuery5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery6.TYPE,
                        Neo4jQuery6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery7.TYPE,
                        Neo4jQuery7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery8.TYPE,
                        Neo4jQuery8.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery9.TYPE,
                        Neo4jQuery9.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery10.TYPE,
                        Neo4jQuery10.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery11.TYPE,
                        Neo4jQuery11.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery12.TYPE,
                        Neo4jQuery12.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery13.TYPE,
                        Neo4jQuery13.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery14.TYPE,
                        Neo4jQuery14.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery1PersonProfile.TYPE,
                        Neo4jShortQuery1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery2PersonPosts.TYPE,
                        Neo4jShortQuery2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery3PersonFriends.TYPE,
                        Neo4jShortQuery3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery4MessageContent.TYPE,
                        Neo4jShortQuery4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery5MessageCreator.TYPE,
                        Neo4jShortQuery5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery6MessageForum.TYPE,
                        Neo4jShortQuery6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery7MessageReplies.TYPE,
                        Neo4jShortQuery7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate1AddPerson.TYPE,
                        Neo4jUpdate1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate2AddPostLike.TYPE,
                        Neo4jUpdate2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate3AddCommentLike.TYPE,
                        Neo4jUpdate3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate4AddForum.TYPE,
                        Neo4jUpdate4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate5AddForumMembership.TYPE,
                        Neo4jUpdate5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate6AddPost.TYPE,
                        Neo4jUpdate6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate7AddComment.TYPE,
                        Neo4jUpdate7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate8AddFriendship.TYPE,
                        Neo4jUpdate8.QUERY_STRING,
                        runtimeType,
                        plannerType );
    }

    private final Int2ObjectMap<AnnotatedQuery> annotatedQueries;

    private SnbInteractiveCypherQueries()
    {
        this.annotatedQueries = new Int2ObjectOpenHashMap<>();
    }

    private SnbInteractiveCypherQueries withQuery(
            int operationType,
            String queryString,
            Runtime runtimeType,
            Planner plannerType )
    {
        annotatedQueries.put(
                operationType,
                new AnnotatedQuery(
                        queryString,
                        plannerType,
                        runtimeType
                )
        );
        return this;
    }

    @Override
    public AnnotatedQuery queryFor( Operation operation ) throws DbException
    {
        if ( !annotatedQueries.containsKey( operation.type() ) )
        {
            throw new DbException( format( "Operation type %s has no associated query", operation.type() ) );
        }
        return annotatedQueries.get( operation.type() );
    }
}
