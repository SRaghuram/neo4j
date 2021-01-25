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
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery10HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery11HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery12HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery13HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery14HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery1HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery2HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery3HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery4HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery5HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery6HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery7HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery8HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.LongQuery9HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.ShortQuery1HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.ShortQuery2HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.ShortQuery3HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.ShortQuery4HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.ShortQuery5HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.ShortQuery6HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.ShortQuery7HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update1HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update2HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update3HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update4HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update5HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update6HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update7HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.interactive.embedded_core_regular.Update8HandlerEmbeddedCoreRegular;
import com.neo4j.bench.ldbc.operators.Warmup;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;

public class SnbInteractiveEmbeddedCoreRegularCommands implements Neo4jDbCommands
{
    private final File homeDir;
    private final File configFile;
    private final LoggingService loggingService;
    private Neo4jConnectionState connection;
    private final boolean doWarmup;

    public SnbInteractiveEmbeddedCoreRegularCommands(
            File homeDir,
            File configFile,
            LoggingService loggingService,
            boolean doWarmup )
    {
        this.homeDir = homeDir;
        this.configFile = configFile;
        this.loggingService = loggingService;
        this.doWarmup = doWarmup;
    }

    @Override
    public void init() throws DbException
    {
        DatabaseManagementService managementService = Neo4jDb.newDb( homeDir, configFile );
        GraphDatabaseService db = managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        LdbcIndexer.waitForIndexesToBeOnline( db );
        registerShutdownHook( managementService );

        GraphMetadataProxy metadata = GraphMetadataProxy.loadFrom( db );
        if ( !metadata.timestampResolution().equals( LdbcDateCodec.Resolution.NOT_APPLICABLE ) ||
             !metadata.neo4jSchema().equals( Neo4jSchema.NEO4J_REGULAR ) )
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
        db.registerOperationHandler( LdbcQuery1.class, LongQuery1HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery2.class, LongQuery2HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery3.class, LongQuery3HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery4.class, LongQuery4HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery5.class, LongQuery5HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery6.class, LongQuery6HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery7.class, LongQuery7HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery8.class, LongQuery8HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery9.class, LongQuery9HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery10.class, LongQuery10HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery11.class, LongQuery11HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery12.class, LongQuery12HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery13.class, LongQuery13HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcQuery14.class, LongQuery14HandlerEmbeddedCoreRegular.class );

        db.registerOperationHandler( LdbcShortQuery1PersonProfile.class, ShortQuery1HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcShortQuery2PersonPosts.class, ShortQuery2HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcShortQuery3PersonFriends.class, ShortQuery3HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcShortQuery4MessageContent.class, ShortQuery4HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcShortQuery5MessageCreator.class, ShortQuery5HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcShortQuery6MessageForum.class, ShortQuery6HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcShortQuery7MessageReplies.class, ShortQuery7HandlerEmbeddedCoreRegular.class );

        db.registerOperationHandler( LdbcUpdate1AddPerson.class, Update1HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcUpdate2AddPostLike.class, Update2HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcUpdate3AddCommentLike.class, Update3HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcUpdate4AddForum.class, Update4HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcUpdate5AddForumMembership.class, Update5HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcUpdate6AddPost.class, Update6HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcUpdate7AddComment.class, Update7HandlerEmbeddedCoreRegular.class );
        db.registerOperationHandler( LdbcUpdate8AddFriendship.class, Update8HandlerEmbeddedCoreRegular.class );
    }

    private static void registerShutdownHook( final DatabaseManagementService managementService )
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                managementService.shutdown();
            }
        } );
    }
}
