/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;
import com.neo4j.bench.ldbc.utils.AnnotatedQuery;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class SnbInteractiveCypherQueries implements AnnotatedQueries
{

    public static SnbInteractiveCypherQueries none()
    {
        return new SnbInteractiveCypherQueries();
    }

    public static SnbInteractiveCypherQueries createWith( PlannerType plannerType, RuntimeType runtimeType )
    {
        return new SnbInteractiveCypherQueries()
                .withQuery(
                        LdbcQuery1.TYPE,
                        LdbcQuery1.class,
                        Neo4jQuery1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery2.TYPE,
                        LdbcQuery2.class,
                        Neo4jQuery2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery3.TYPE,
                        LdbcQuery3.class,
                        Neo4jQuery3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery4.TYPE,
                        LdbcQuery4.class,
                        Neo4jQuery4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery5.TYPE,
                        LdbcQuery5.class,
                        Neo4jQuery5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery6.TYPE,
                        LdbcQuery6.class,
                        Neo4jQuery6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery7.TYPE,
                        LdbcQuery7.class,
                        Neo4jQuery7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery8.TYPE,
                        LdbcQuery8.class,
                        Neo4jQuery8.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery9.TYPE,
                        LdbcQuery9.class,
                        Neo4jQuery9.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery10.TYPE,
                        LdbcQuery10.class,
                        Neo4jQuery10.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery11.TYPE,
                        LdbcQuery11.class,
                        Neo4jQuery11.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery12.TYPE,
                        LdbcQuery12.class,
                        Neo4jQuery12.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery13.TYPE,
                        LdbcQuery13.class,
                        Neo4jQuery13.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcQuery14.TYPE,
                        LdbcQuery14.class,
                        Neo4jQuery14.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery1PersonProfile.TYPE,
                        LdbcShortQuery1PersonProfile.class,
                        Neo4jShortQuery1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery2PersonPosts.TYPE,
                        LdbcShortQuery2PersonPosts.class,
                        Neo4jShortQuery2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery3PersonFriends.TYPE,
                        LdbcShortQuery3PersonFriends.class,
                        Neo4jShortQuery3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery4MessageContent.TYPE,
                        LdbcShortQuery4MessageContent.class,
                        Neo4jShortQuery4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery5MessageCreator.TYPE,
                        LdbcShortQuery5MessageCreator.class,
                        Neo4jShortQuery5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery6MessageForum.TYPE,
                        LdbcShortQuery6MessageForum.class,
                        Neo4jShortQuery6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcShortQuery7MessageReplies.TYPE,
                        LdbcShortQuery7MessageReplies.class,
                        Neo4jShortQuery7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate1AddPerson.TYPE,
                        LdbcUpdate1AddPerson.class,
                        Neo4jUpdate1.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate2AddPostLike.TYPE,
                        LdbcUpdate2AddPostLike.class,
                        Neo4jUpdate2.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate3AddCommentLike.TYPE,
                        LdbcUpdate3AddCommentLike.class,
                        Neo4jUpdate3.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate4AddForum.TYPE,
                        LdbcUpdate4AddForum.class,
                        Neo4jUpdate4.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate5AddForumMembership.TYPE,
                        LdbcUpdate5AddForumMembership.class,
                        Neo4jUpdate5.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate6AddPost.TYPE,
                        LdbcUpdate6AddPost.class,
                        Neo4jUpdate6.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate7AddComment.TYPE,
                        LdbcUpdate7AddComment.class,
                        Neo4jUpdate7.QUERY_STRING,
                        runtimeType,
                        plannerType )
                .withQuery(
                        LdbcUpdate8AddFriendship.TYPE,
                        LdbcUpdate8AddFriendship.class,
                        Neo4jUpdate8.QUERY_STRING,
                        runtimeType,
                        plannerType );
    }

    private final Int2ObjectMap<AnnotatedQuery> annotatedQueries;

    public SnbInteractiveCypherQueries()
    {
        this.annotatedQueries = new Int2ObjectOpenHashMap<>();
    }

    private SnbInteractiveCypherQueries withQuery(
            int operationType,
            Class<? extends Operation> operationClass,
            String queryString,
            RuntimeType runtimeType,
            PlannerType plannerType )
    {
        annotatedQueries.put(
                operationType,
                new AnnotatedQuery(
                        operationType,
                        operationClass.getSimpleName(),
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

    @Override
    public Iterable<AnnotatedQuery> allQueries() throws DbException
    {
        return annotatedQueries.entrySet().stream().map( Map.Entry::getValue ).collect( toList() );
    }
}
