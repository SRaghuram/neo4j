/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.queries;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
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
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.Neo4jQuery;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveCypherQueries;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery10EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery11EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery12EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery13EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery14EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery1EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery2EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery3EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery4EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery5EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery6EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery7EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery8EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery9EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery1EmbeddedCore_0_1_2;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery2EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery3EmbeddedCore_0_1_2;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery4EmbeddedCore_0_1_2;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery5EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery6EmbeddedCore_0_1_2;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery7EmbeddedCore_0_1;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update1EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update2EmbeddedCore_0_1_2;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update3EmbeddedCore_0_1_2;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update4EmbeddedCore_0_1_2;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update5EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update6EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update7EmbeddedCore_0;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update8EmbeddedCore_0_1_2;

import java.io.File;
import java.util.List;

import org.neo4j.graphdb.Transaction;

public class SnbInteractiveQueryCorrectnessEmbeddedCoreRegularTest
        extends SnbInteractiveQueryCorrectnessTest<Neo4jConnectionState>
{

    private <OPERATION_RESULT, OPERATION extends Operation<OPERATION_RESULT>> OPERATION_RESULT executeQuery(
            OPERATION operation,
            Neo4jQuery<OPERATION,OPERATION_RESULT,Neo4jConnectionState> query,
            Neo4jConnectionState neo4jConnectionState ) throws DbException
    {
        // TODO uncomment to print query
        System.out.println( operation.toString() + "\n" + query.getClass().getSimpleName() + "\n" );
        OPERATION_RESULT results;
        try ( Transaction tx = neo4jConnectionState.db().beginTx() )
        {
            results = query.execute( neo4jConnectionState, operation );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new DbException( "Error executing query", e );
        }
        return results;
    }

    private <OPERATION_RESULT, OPERATION extends Operation<OPERATION_RESULT>> void executeUpdate(
            OPERATION operation,
            Neo4jQuery<OPERATION,OPERATION_RESULT,Neo4jConnectionState> query,
            Neo4jConnectionState neo4jConnectionState ) throws DbException
    {
        // TODO uncomment to print query
        System.out.println( operation.toString() + "\n" + query.getClass().getSimpleName() + "\n" );
        try ( Transaction tx = neo4jConnectionState.db().beginTx() )
        {
            query.execute( neo4jConnectionState, operation );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new DbException( "Error executing query", e );
        }
    }

    @Override
    public Neo4jConnectionState openConnection( String path ) throws Exception
    {
        return new Neo4jConnectionState(
                Neo4jDb.newDb( new File( path ), DriverConfigUtils.neo4jTestConfig() ),
                null,
                null,
                new Log4jLoggingServiceFactory( true ).loggingServiceFor( "TEST" ),
                SnbInteractiveCypherQueries.none(),
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.NOT_APPLICABLE
        );
    }

    @Override
    public void closeConnection( Neo4jConnectionState connection ) throws Exception
    {
        connection.db().shutdown();
    }

    @Override
    public List<LdbcQuery1Result> neo4jLongQuery1Impl( Neo4jConnectionState connection, LdbcQuery1 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery1EmbeddedCore_0(), connection );
    }

    @Override
    public List<LdbcQuery2Result> neo4jLongQuery2Impl( Neo4jConnectionState connection, LdbcQuery2 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery2EmbeddedCore_0(), connection );
    }

    @Override
    public List<LdbcQuery3Result> neo4jLongQuery3Impl( Neo4jConnectionState connection, LdbcQuery3 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery3EmbeddedCore_0(), connection );
    }

    @Override
    public List<LdbcQuery4Result> neo4jLongQuery4Impl( Neo4jConnectionState connection, LdbcQuery4 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery4EmbeddedCore_0_1(), connection );
    }

    @Override
    public List<LdbcQuery5Result> neo4jLongQuery5Impl( Neo4jConnectionState connection, LdbcQuery5 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery5EmbeddedCore_0(), connection );
    }

    @Override
    public List<LdbcQuery6Result> neo4jLongQuery6Impl( Neo4jConnectionState connection, LdbcQuery6 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery6EmbeddedCore_0_1(), connection );
    }

    @Override
    public List<LdbcQuery7Result> neo4jLongQuery7Impl( Neo4jConnectionState connection, LdbcQuery7 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery7EmbeddedCore_0_1(), connection );
    }

    @Override
    public List<LdbcQuery8Result> neo4jLongQuery8Impl( Neo4jConnectionState connection, LdbcQuery8 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery8EmbeddedCore_0_1(), connection );
    }

    @Override
    public List<LdbcQuery9Result> neo4jLongQuery9Impl( Neo4jConnectionState connection, LdbcQuery9 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery9EmbeddedCore_0(), connection );
    }

    @Override
    public List<LdbcQuery10Result> neo4jLongQuery10Impl( Neo4jConnectionState connection, LdbcQuery10 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery10EmbeddedCore_0_1(), connection );
    }

    @Override
    public List<LdbcQuery11Result> neo4jLongQuery11Impl( Neo4jConnectionState connection, LdbcQuery11 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery11EmbeddedCore_0(), connection );
    }

    @Override
    public List<LdbcQuery12Result> neo4jLongQuery12Impl( Neo4jConnectionState connection, LdbcQuery12 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery12EmbeddedCore_0_1(), connection );
    }

    @Override
    public LdbcQuery13Result neo4jLongQuery13Impl( Neo4jConnectionState connection, LdbcQuery13 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery13EmbeddedCore_0_1(), connection );
    }

    @Override
    public List<LdbcQuery14Result> neo4jLongQuery14Impl( Neo4jConnectionState connection, LdbcQuery14 operation )
            throws Exception
    {
        return executeQuery( operation, new LongQuery14EmbeddedCore_0(), connection );
    }

    @Override
    public void neo4jUpdate1Impl( Neo4jConnectionState connection, LdbcUpdate1AddPerson operation )
            throws Exception
    {
        executeUpdate( operation, new Update1EmbeddedCore_0(), connection );
    }

    @Override
    public void neo4jUpdate2Impl( Neo4jConnectionState connection, LdbcUpdate2AddPostLike operation )
            throws Exception
    {
        executeUpdate( operation, new Update2EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public void neo4jUpdate3Impl( Neo4jConnectionState connection, LdbcUpdate3AddCommentLike operation )
            throws Exception
    {
        executeUpdate( operation, new Update3EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public void neo4jUpdate4Impl( Neo4jConnectionState connection, LdbcUpdate4AddForum operation ) throws Exception
    {
        executeUpdate( operation, new Update4EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public void neo4jUpdate5Impl( Neo4jConnectionState connection, LdbcUpdate5AddForumMembership operation )
            throws Exception
    {
        executeUpdate( operation, new Update5EmbeddedCore_0(), connection );
    }

    @Override
    public void neo4jUpdate6Impl( Neo4jConnectionState connection, LdbcUpdate6AddPost operation ) throws Exception
    {
        executeUpdate( operation, new Update6EmbeddedCore_0(), connection );
    }

    @Override
    public void neo4jUpdate7Impl( Neo4jConnectionState connection, LdbcUpdate7AddComment operation )
            throws Exception
    {
        executeUpdate( operation, new Update7EmbeddedCore_0(), connection );
    }

    @Override
    public void neo4jUpdate8Impl( Neo4jConnectionState connection, LdbcUpdate8AddFriendship operation )
            throws Exception
    {
        executeUpdate( operation, new Update8EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public LdbcShortQuery1PersonProfileResult neo4jShortQuery1Impl( Neo4jConnectionState connection,
            LdbcShortQuery1PersonProfile operation ) throws Exception
    {
        return executeQuery( operation, new ShortQuery1EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public List<LdbcShortQuery2PersonPostsResult> neo4jShortQuery2Impl( Neo4jConnectionState connection,
            LdbcShortQuery2PersonPosts operation ) throws Exception
    {
        return executeQuery( operation, new ShortQuery2EmbeddedCore_0_1(), connection );
    }

    @Override
    public List<LdbcShortQuery3PersonFriendsResult> neo4jShortQuery3Impl( Neo4jConnectionState connection,
            LdbcShortQuery3PersonFriends operation ) throws Exception
    {
        return executeQuery( operation, new ShortQuery3EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public LdbcShortQuery4MessageContentResult neo4jShortQuery4Impl( Neo4jConnectionState connection,
            LdbcShortQuery4MessageContent operation ) throws Exception
    {
        return executeQuery( operation, new ShortQuery4EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public LdbcShortQuery5MessageCreatorResult neo4jShortQuery5Impl( Neo4jConnectionState connection,
            LdbcShortQuery5MessageCreator operation ) throws Exception
    {
        return executeQuery( operation, new ShortQuery5EmbeddedCore_0_1(), connection );
    }

    @Override
    public LdbcShortQuery6MessageForumResult neo4jShortQuery6Impl( Neo4jConnectionState connection,
            LdbcShortQuery6MessageForum operation ) throws Exception
    {
        return executeQuery( operation, new ShortQuery6EmbeddedCore_0_1_2(), connection );
    }

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> neo4jShortQuery7Impl( Neo4jConnectionState connection,
            LdbcShortQuery7MessageReplies operation ) throws Exception
    {
        return executeQuery( operation, new ShortQuery7EmbeddedCore_0_1(), connection );
    }
}
