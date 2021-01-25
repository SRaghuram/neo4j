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
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery10HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery11HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery12HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery13HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery14HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery1HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery2HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery3HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery4HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery5HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery6HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery7HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery8HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcQuery9HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcShortQuery1HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcShortQuery2HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcShortQuery3HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcShortQuery4HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcShortQuery5HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcShortQuery6HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcShortQuery7HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate1HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate2HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate3HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate4HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate5HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate6HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate7HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.interactive.embedded_cypher_regular.LdbcUpdate8HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.operators.Warmup;
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;

public class SnbInteractiveEmbeddedCypherRegularCommands implements Neo4jDbCommands
{
    private final File homeDir;
    private final File configFile;
    private final LoggingService loggingService;
    private Neo4jConnectionState connection;
    private final AnnotatedQueries annotatedQueries;
    private final boolean doWarmup;

    public SnbInteractiveEmbeddedCypherRegularCommands(
            File dbDir,
            File configFile,
            LoggingService loggingService,
            AnnotatedQueries annotatedQueries,
            boolean doWarmup )
    {
        this.homeDir = dbDir;
        this.configFile = configFile;
        this.loggingService = loggingService;
        this.annotatedQueries = annotatedQueries;
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
                annotatedQueries,
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
        db.registerOperationHandler( LdbcQuery1.class, LdbcQuery1HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery2.class, LdbcQuery2HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery3.class, LdbcQuery3HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery4.class, LdbcQuery4HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery5.class, LdbcQuery5HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery6.class, LdbcQuery6HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery7.class, LdbcQuery7HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery8.class, LdbcQuery8HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery9.class, LdbcQuery9HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery10.class, LdbcQuery10HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery11.class, LdbcQuery11HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery12.class, LdbcQuery12HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery13.class, LdbcQuery13HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery14.class, LdbcQuery14HandlerEmbeddedCypher.class );

        db.registerOperationHandler( LdbcShortQuery1PersonProfile.class, LdbcShortQuery1HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcShortQuery2PersonPosts.class, LdbcShortQuery2HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcShortQuery3PersonFriends.class, LdbcShortQuery3HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcShortQuery4MessageContent.class, LdbcShortQuery4HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcShortQuery5MessageCreator.class, LdbcShortQuery5HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcShortQuery6MessageForum.class, LdbcShortQuery6HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcShortQuery7MessageReplies.class, LdbcShortQuery7HandlerEmbeddedCypher.class );

        db.registerOperationHandler( LdbcUpdate1AddPerson.class, LdbcUpdate1HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcUpdate2AddPostLike.class, LdbcUpdate2HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcUpdate3AddCommentLike.class, LdbcUpdate3HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcUpdate4AddForum.class, LdbcUpdate4HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcUpdate5AddForumMembership.class, LdbcUpdate5HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcUpdate6AddPost.class, LdbcUpdate6HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcUpdate7AddComment.class, LdbcUpdate7HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcUpdate8AddFriendship.class, LdbcUpdate8HandlerEmbeddedCypher.class );
    }

    private static void registerShutdownHook( final DatabaseManagementService managementService )
    {
        Runtime.getRuntime().addShutdownHook( new Thread( managementService::shutdown ) );
    }
}
