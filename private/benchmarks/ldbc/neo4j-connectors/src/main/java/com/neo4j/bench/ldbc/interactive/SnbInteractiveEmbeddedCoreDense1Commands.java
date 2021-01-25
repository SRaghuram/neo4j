/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
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
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.Neo4jDbCommands;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery10HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery11HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery12HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery13HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery14HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery1HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery2HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery3HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery4HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery5HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery6HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery7HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery8HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.LongQuery9HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.ShortQuery1HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.ShortQuery2HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.ShortQuery3HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.ShortQuery4HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.ShortQuery5HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.ShortQuery6HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.ShortQuery7HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update1HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update2HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update3HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update4HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update5HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update6HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update7HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.interactive.embedded_core_dense1.Update8HandlerEmbeddedCoreDense1;
import com.neo4j.bench.ldbc.operators.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;

public class SnbInteractiveEmbeddedCoreDense1Commands implements Neo4jDbCommands
{
    private final File homeDir;
    private final File configFile;
    private Neo4jConnectionState connection;
    private final LoggingService loggingService;
    private final boolean doWarmup;

    public SnbInteractiveEmbeddedCoreDense1Commands(
            File dbDir,
            File configFile,
            LoggingService loggingService,
            boolean doWarmup )
    {
        this.homeDir = dbDir;
        this.configFile = configFile;
        this.loggingService = loggingService;
        this.doWarmup = doWarmup;
    }

    @Override
    public void init() throws DbException
    {
        DatabaseManagementService managementService = Neo4jDb.newDb( homeDir, configFile);
        GraphDatabaseService db = managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        LdbcIndexer.waitForIndexesToBeOnline( db );
        registerShutdownHook( managementService );

        GraphMetadataProxy metadata = GraphMetadataProxy.loadFrom( db );
        if ( !metadata.hasCommentHasCreatorMinDateAtResolution() ||
             !metadata.hasCommentHasCreatorMaxDateAtResolution() ||
             !metadata.hasPostHasCreatorMinDateAtResolution() ||
             !metadata.hasPostHasCreatorMaxDateAtResolution() )
        {
            throw new DbException( format( "HAS CREATOR relationship timestamps missing\n%s", metadata.toString() ) );
        }
        if ( !metadata.hasWorkFromMinYear() ||
             !metadata.hasWorkFromMaxYear() )
        {
            throw new DbException( format( "WORKS FROM relationship timestamps missing\n%s", metadata.toString() ) );
        }
        if ( !metadata.hasCommentIsLocatedInMinDateAtResolution() ||
             !metadata.hasCommentIsLocatedInMaxDateAtResolution() ||
             !metadata.hasPostIsLocatedInMinDateAtResolution() ||
             !metadata.hasPostIsLocatedInMaxDateAtResolution() )
        {
            throw new DbException( format( "IS LOCATED IN relationship timestamps missing\n%s", metadata.toString() ) );
        }
        if ( !metadata.hasHasMemberMinDateAtResolution() ||
             !metadata.hasHasMemberMaxDateAtResolution() )
        {
            throw new DbException( format( "HAS MEMBER relationship timestamps missing\n%s", metadata.toString() ) );
        }
        if ( metadata.timestampResolution().equals( LdbcDateCodec.Resolution.NOT_APPLICABLE ) ||
             !metadata.neo4jSchema().equals( Neo4jSchema.NEO4J_DENSE_1 ) )
        {
            throw new DbException( format( "Incompatible schema\n%s", metadata.toString() ) );
        }
        loggingService.info( metadata.toString() );

        connection = new Neo4jConnectionState( managementService, db,
                null,
                null,
                loggingService,
                SnbInteractiveCypherQueries.none(),
                metadata.dateFormat(),
                metadata.timestampResolution()
        );

        Calendar calendar = connection.calendar();
        QueryDateUtil dateUtil = connection.dateUtil();

        connection.timeStampedRelationshipTypesCache().resizeWorksAtForNewYear(
                metadata.workFromMinYear() );
        connection.timeStampedRelationshipTypesCache().resizeWorksAtForNewYear(
                metadata.workFromMaxYear() );

        connection.timeStampedRelationshipTypesCache().resizeCommentHasCreatorForNewDate(
                metadata.commentHasCreatorMinDateAtResolution(),
                calendar,
                dateUtil );
        connection.timeStampedRelationshipTypesCache().resizeCommentHasCreatorForNewDate(
                metadata.commentHasCreatorMaxDateAtResolution(),
                calendar,
                dateUtil );

        connection.timeStampedRelationshipTypesCache().resizePostHasCreatorForNewDate(
                metadata.postHasCreatorMinDateAtResolution(),
                calendar,
                dateUtil );
        connection.timeStampedRelationshipTypesCache().resizePostHasCreatorForNewDate(
                metadata.postHasCreatorMaxDateAtResolution(),
                calendar,
                dateUtil );

        connection.timeStampedRelationshipTypesCache().resizeCommentIsLocatedInForNewDate(
                metadata.commentIsLocatedInMinDateAtResolution(),
                calendar,
                dateUtil );
        connection.timeStampedRelationshipTypesCache().resizeCommentIsLocatedInForNewDate(
                metadata.commentIsLocatedInMaxDateAtResolution(),
                calendar,
                dateUtil );

        connection.timeStampedRelationshipTypesCache().resizePostIsLocatedInForNewDate(
                metadata.postIsLocatedInMinDateAtResolution(),
                calendar,
                dateUtil );
        connection.timeStampedRelationshipTypesCache().resizePostIsLocatedInForNewDate(
                metadata.postIsLocatedInMaxDateAtResolution(),
                calendar,
                dateUtil );

        connection.timeStampedRelationshipTypesCache().resizeHasMemberForNewDate(
                metadata.hasMemberMinDateAtResolution(),
                calendar,
                dateUtil );
        connection.timeStampedRelationshipTypesCache().resizeHasMemberForNewDate(
                metadata.hasMemberMaxDateAtResolution(),
                calendar,
                dateUtil );

        if ( doWarmup )
        {
            Warmup.warmup( (GraphDatabaseAPI) db, loggingService );
        }
    }

    @Override
    public void close() throws IOException
    {
        connection.close();
    }

    @Override
    public DbConnectionState getConnectionState()
    {
        return connection;
    }

    @Override
    public void registerHandlersWithDb( Db db ) throws DbException
    {
        db.registerOperationHandler( LdbcQuery1.class, LongQuery1HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery2.class, LongQuery2HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery3.class, LongQuery3HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery4.class, LongQuery4HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery5.class, LongQuery5HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery6.class, LongQuery6HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery7.class, LongQuery7HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery8.class, LongQuery8HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery9.class, LongQuery9HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery10.class, LongQuery10HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery11.class, LongQuery11HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery12.class, LongQuery12HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery13.class, LongQuery13HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcQuery14.class, LongQuery14HandlerEmbeddedCoreDense1.class );

        db.registerOperationHandler( LdbcShortQuery1PersonProfile.class,
                ShortQuery1HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcShortQuery2PersonPosts.class,
                ShortQuery2HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcShortQuery3PersonFriends.class,
                ShortQuery3HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcShortQuery4MessageContent.class,
                ShortQuery4HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcShortQuery5MessageCreator.class,
                ShortQuery5HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcShortQuery6MessageForum.class,
                ShortQuery6HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcShortQuery7MessageReplies.class,
                ShortQuery7HandlerEmbeddedCoreDense1.class );

        db.registerOperationHandler( LdbcUpdate1AddPerson.class,
                Update1HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcUpdate2AddPostLike.class,
                Update2HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcUpdate3AddCommentLike.class,
                Update3HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcUpdate4AddForum.class,
                Update4HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcUpdate5AddForumMembership.class,
                Update5HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcUpdate6AddPost.class,
                Update6HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcUpdate7AddComment.class,
                Update7HandlerEmbeddedCoreDense1.class );
        db.registerOperationHandler( LdbcUpdate8AddFriendship.class,
                Update8HandlerEmbeddedCoreDense1.class );
    }

    private static void registerShutdownHook( final DatabaseManagementService managementService )
    {
        Runtime.getRuntime().addShutdownHook(
                new Thread()
                {
                    @Override
                    public void run()
                    {
                        managementService.shutdown();
                    }
                }
        );
    }
}
