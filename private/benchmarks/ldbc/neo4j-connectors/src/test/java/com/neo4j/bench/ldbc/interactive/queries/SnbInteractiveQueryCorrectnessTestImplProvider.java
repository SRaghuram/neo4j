/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.queries;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.io.File;
import java.util.List;

public interface SnbInteractiveQueryCorrectnessTestImplProvider<CONNECTION>
{
    CONNECTION openConnection( File dbDir, File configDir ) throws Exception;

    void closeConnection( CONNECTION connection ) throws Exception;

    List<LdbcQuery1Result> neo4jLongQuery1Impl( CONNECTION connection, LdbcQuery1 operation ) throws Exception;

    List<LdbcQuery2Result> neo4jLongQuery2Impl( CONNECTION connection, LdbcQuery2 operation ) throws Exception;

    List<LdbcQuery3Result> neo4jLongQuery3Impl( CONNECTION connection, LdbcQuery3 operation ) throws Exception;

    List<LdbcQuery4Result> neo4jLongQuery4Impl( CONNECTION connection, LdbcQuery4 operation ) throws Exception;

    List<LdbcQuery5Result> neo4jLongQuery5Impl( CONNECTION connection, LdbcQuery5 operation ) throws Exception;

    List<LdbcQuery6Result> neo4jLongQuery6Impl( CONNECTION connection, LdbcQuery6 operation ) throws Exception;

    List<LdbcQuery7Result> neo4jLongQuery7Impl( CONNECTION connection, LdbcQuery7 operation ) throws Exception;

    List<LdbcQuery8Result> neo4jLongQuery8Impl( CONNECTION connection, LdbcQuery8 operation ) throws Exception;

    List<LdbcQuery9Result> neo4jLongQuery9Impl( CONNECTION connection, LdbcQuery9 operation ) throws Exception;

    List<LdbcQuery10Result> neo4jLongQuery10Impl( CONNECTION connection, LdbcQuery10 operation ) throws Exception;

    List<LdbcQuery11Result> neo4jLongQuery11Impl( CONNECTION connection, LdbcQuery11 operation ) throws Exception;

    List<LdbcQuery12Result> neo4jLongQuery12Impl( CONNECTION connection, LdbcQuery12 operation ) throws Exception;

    LdbcQuery13Result neo4jLongQuery13Impl( CONNECTION connection, LdbcQuery13 operation ) throws Exception;

    List<LdbcQuery14Result> neo4jLongQuery14Impl( CONNECTION connection, LdbcQuery14 operation ) throws Exception;

    void neo4jUpdate1Impl( CONNECTION connection, LdbcUpdate1AddPerson operation ) throws Exception;

    void neo4jUpdate2Impl( CONNECTION connection, LdbcUpdate2AddPostLike operation ) throws Exception;

    void neo4jUpdate3Impl( CONNECTION connection, LdbcUpdate3AddCommentLike operation ) throws Exception;

    void neo4jUpdate4Impl( CONNECTION connection, LdbcUpdate4AddForum operation ) throws Exception;

    void neo4jUpdate5Impl( CONNECTION connection, LdbcUpdate5AddForumMembership operation ) throws Exception;

    void neo4jUpdate6Impl( CONNECTION connection, LdbcUpdate6AddPost operation ) throws Exception;

    void neo4jUpdate7Impl( CONNECTION connection, LdbcUpdate7AddComment operation ) throws Exception;

    void neo4jUpdate8Impl( CONNECTION connection, LdbcUpdate8AddFriendship operation ) throws Exception;

    LdbcShortQuery1PersonProfileResult neo4jShortQuery1Impl( CONNECTION connection,
            LdbcShortQuery1PersonProfile operation ) throws Exception;

    List<LdbcShortQuery2PersonPostsResult> neo4jShortQuery2Impl( CONNECTION connection,
            LdbcShortQuery2PersonPosts operation ) throws Exception;

    List<LdbcShortQuery3PersonFriendsResult> neo4jShortQuery3Impl( CONNECTION connection,
            LdbcShortQuery3PersonFriends operation ) throws Exception;

    LdbcShortQuery4MessageContentResult neo4jShortQuery4Impl( CONNECTION connection,
            LdbcShortQuery4MessageContent operation ) throws Exception;

    LdbcShortQuery5MessageCreatorResult neo4jShortQuery5Impl( CONNECTION connection,
            LdbcShortQuery5MessageCreator operation ) throws Exception;

    LdbcShortQuery6MessageForumResult neo4jShortQuery6Impl( CONNECTION connection,
            LdbcShortQuery6MessageForum operation ) throws Exception;

    List<LdbcShortQuery7MessageRepliesResult> neo4jShortQuery7Impl( CONNECTION connection,
            LdbcShortQuery7MessageReplies operation ) throws Exception;
}
