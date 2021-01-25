/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.Neo4jQuery;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveCypherQueries;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery10RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery11RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery12RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery13RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery14RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery1RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery2RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery3RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery4RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery5RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery6RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery7RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery8RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jLongQuery9RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jShortQuery1RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jShortQuery2RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jShortQuery3RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jShortQuery4RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jShortQuery5RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jShortQuery6RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jShortQuery7RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate1RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate2RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate3RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate4RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate5RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate6RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate7RemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.Neo4jUpdate8RemoteCypher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.List;

import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class SnbInteractiveQueryCorrectnessRemoteCypherDefaultTest
        extends SnbInteractiveQueryCorrectnessTest<Neo4jConnectionState>
{
    private static final Logger LOG = LoggerFactory.getLogger( SnbInteractiveQueryCorrectnessRemoteCypherDefaultTest.class );

    private <OPERATION_RESULT, OPERATION extends Operation<OPERATION_RESULT>> OPERATION_RESULT executeQuery(
            OPERATION operation,
            Neo4jQuery<OPERATION,OPERATION_RESULT,Neo4jConnectionState> query,
            Neo4jConnectionState connectionState ) throws DbException
    {
        // TODO uncomment to print query
        LOG.debug( operation.toString() );
        LOG.debug( query.getClass().getSimpleName() + "\n" );
        OPERATION_RESULT results;
        results = query.execute( connectionState, operation );
        return results;
    }

    private <OPERATION_RESULT, OPERATION extends Operation<OPERATION_RESULT>> void executeUpdate(
            OPERATION operation,
            Neo4jQuery<OPERATION,OPERATION_RESULT,Neo4jConnectionState> query,
            Neo4jConnectionState connectionState ) throws DbException
    {
        // TODO uncomment to print query
        LOG.debug( operation.toString() );
        LOG.debug( query.getClass().getSimpleName() + "\n" );
        try ( Transaction tx = connectionState.beginTx() )
        {
            query.execute( connectionState, operation );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new DbException( "Error executing query", e );
        }
        finally
        {
            connectionState.freeTx();
        }
    }

    @Override
    public Neo4jConnectionState openConnection( File dbDir, File configDir ) throws Exception
    {
        try
        {
            DatabaseManagementService managementService = Neo4jDb.newDbBuilderForBolt(
                    dbDir,
                        DriverConfigUtils.neo4jTestConfig( configDir ),
                        "localhost",
                        0
                ).build();
            GraphDatabaseService graphDatabase = managementService.database( DEFAULT_DATABASE_NAME );
            ConnectorPortRegister portRegister = ((GraphDatabaseAPI) graphDatabase).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
            return new Neo4jConnectionState( managementService, graphDatabase,
                    URI.create( "bolt://" + portRegister.getLocalAddress( "bolt" ) ),
                    AuthTokens.none(),
                    new Log4jLoggingServiceFactory( true ).loggingServiceFor( "TEST" ),
                    SnbInteractiveCypherQueries.createWith( Planner.DEFAULT, Runtime.DEFAULT ),
                    LdbcDateCodec.Format.NUMBER_UTC,
                    LdbcDateCodec.Resolution.NOT_APPLICABLE
            );
        }
        catch ( Throwable e )
        {
            throw new DbException( "Could not create database", e );
        }
    }

    @Override
    public void closeConnection( Neo4jConnectionState connectionState ) throws Exception
    {
        connectionState.close();
    }

    @Override
    public List<LdbcQuery1Result> neo4jLongQuery1Impl( Neo4jConnectionState connectionState,
            LdbcQuery1 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery1RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery2Result> neo4jLongQuery2Impl( Neo4jConnectionState connectionState,
            LdbcQuery2 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery2RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery3Result> neo4jLongQuery3Impl( Neo4jConnectionState connectionState,
            LdbcQuery3 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery3RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery4Result> neo4jLongQuery4Impl( Neo4jConnectionState connectionState,
            LdbcQuery4 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery4RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery5Result> neo4jLongQuery5Impl( Neo4jConnectionState connectionState,
            LdbcQuery5 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery5RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery6Result> neo4jLongQuery6Impl( Neo4jConnectionState connectionState,
            LdbcQuery6 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery6RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery7Result> neo4jLongQuery7Impl( Neo4jConnectionState connectionState,
            LdbcQuery7 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery7RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery8Result> neo4jLongQuery8Impl( Neo4jConnectionState connectionState,
            LdbcQuery8 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery8RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery9Result> neo4jLongQuery9Impl( Neo4jConnectionState connectionState,
            LdbcQuery9 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery9RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery10Result> neo4jLongQuery10Impl( Neo4jConnectionState connectionState,
            LdbcQuery10 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery10RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery11Result> neo4jLongQuery11Impl( Neo4jConnectionState connectionState,
            LdbcQuery11 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery11RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery12Result> neo4jLongQuery12Impl( Neo4jConnectionState connectionState,
            LdbcQuery12 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery12RemoteCypher(), connectionState );
    }

    @Override
    public LdbcQuery13Result neo4jLongQuery13Impl( Neo4jConnectionState connectionState, LdbcQuery13 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery13RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcQuery14Result> neo4jLongQuery14Impl( Neo4jConnectionState connectionState,
            LdbcQuery14 operation )
            throws Exception
    {
        return executeQuery( operation, new Neo4jLongQuery14RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate1Impl( Neo4jConnectionState connectionState, LdbcUpdate1AddPerson operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate1RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate2Impl( Neo4jConnectionState connectionState, LdbcUpdate2AddPostLike operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate2RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate3Impl( Neo4jConnectionState connectionState, LdbcUpdate3AddCommentLike operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate3RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate4Impl( Neo4jConnectionState connectionState, LdbcUpdate4AddForum operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate4RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate5Impl( Neo4jConnectionState connectionState,
            LdbcUpdate5AddForumMembership operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate5RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate6Impl( Neo4jConnectionState connectionState, LdbcUpdate6AddPost operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate6RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate7Impl( Neo4jConnectionState connectionState, LdbcUpdate7AddComment operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate7RemoteCypher(), connectionState );
    }

    @Override
    public void neo4jUpdate8Impl( Neo4jConnectionState connectionState, LdbcUpdate8AddFriendship operation )
            throws Exception
    {
        executeUpdate( operation, new Neo4jUpdate8RemoteCypher(), connectionState );
    }

    @Override
    public LdbcShortQuery1PersonProfileResult neo4jShortQuery1Impl( Neo4jConnectionState connectionState,
            LdbcShortQuery1PersonProfile operation ) throws Exception
    {
        return executeQuery( operation, new Neo4jShortQuery1RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcShortQuery2PersonPostsResult> neo4jShortQuery2Impl( Neo4jConnectionState connectionState,
            LdbcShortQuery2PersonPosts operation ) throws Exception
    {
        return executeQuery( operation, new Neo4jShortQuery2RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcShortQuery3PersonFriendsResult> neo4jShortQuery3Impl( Neo4jConnectionState connectionState,
            LdbcShortQuery3PersonFriends operation ) throws Exception
    {
        return executeQuery( operation, new Neo4jShortQuery3RemoteCypher(), connectionState );
    }

    @Override
    public LdbcShortQuery4MessageContentResult neo4jShortQuery4Impl( Neo4jConnectionState connectionState,
            LdbcShortQuery4MessageContent operation ) throws Exception
    {
        return executeQuery( operation, new Neo4jShortQuery4RemoteCypher(), connectionState );
    }

    @Override
    public LdbcShortQuery5MessageCreatorResult neo4jShortQuery5Impl( Neo4jConnectionState connectionState,
            LdbcShortQuery5MessageCreator operation ) throws Exception
    {
        return executeQuery( operation, new Neo4jShortQuery5RemoteCypher(), connectionState );
    }

    @Override
    public LdbcShortQuery6MessageForumResult neo4jShortQuery6Impl( Neo4jConnectionState connectionState,
            LdbcShortQuery6MessageForum operation ) throws Exception
    {
        return executeQuery( operation, new Neo4jShortQuery6RemoteCypher(), connectionState );
    }

    @Override
    public List<LdbcShortQuery7MessageRepliesResult> neo4jShortQuery7Impl( Neo4jConnectionState connectionState,
            LdbcShortQuery7MessageReplies operation ) throws Exception
    {
        return executeQuery( operation, new Neo4jShortQuery7RemoteCypher(), connectionState );
    }
}
