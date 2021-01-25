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
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery10HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery11HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery12HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery13HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery14HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery1HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery2HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery3HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery4HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery5HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery6HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery7HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery8HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcQuery9HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcShortQuery1HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcShortQuery2HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcShortQuery3HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcShortQuery4HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcShortQuery5HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcShortQuery6HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcShortQuery7HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate1HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate2HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate3HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate4HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate5HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate6HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate7HandlerRemoteCypher;
import com.neo4j.bench.ldbc.interactive.remote_cypher_regular.LdbcUpdate8HandlerRemoteCypher;
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.graphdb.GraphDatabaseService;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class SnbInteractiveRemoteCypherRegularCommands implements Neo4jDbCommands
{
    private static final Logger LOG = LoggerFactory.getLogger( SnbInteractiveRemoteCypherRegularCommands.class );

    private final URI uri;
    private final AuthToken authToken;
    private final LoggingService loggingService;
    private final AnnotatedQueries annotatedQueries;
    private Neo4jConnectionState dbConnectionState;
    private final File homeDir;
    private final File configFile;

    public SnbInteractiveRemoteCypherRegularCommands(
            URI uri,
            AuthToken authToken,
            LoggingService loggingService,
            AnnotatedQueries annotatedQueries,
            File dbDir,
            File configFile )
    {
        this.uri = (null != uri) ? uri : URI.create( "bolt://localhost:7687" );
        this.authToken = authToken != null ? authToken : AuthTokens.none();
        this.loggingService = loggingService;
        this.annotatedQueries = annotatedQueries;
        this.homeDir = dbDir;
        this.configFile = configFile;
    }

    @Override
    public void init() throws DbException
    {
        if ( null != homeDir )
        {
            DatabaseManagementService managementService = Neo4jDb.newDbBuilderForBolt( homeDir, configFile, uri ).build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            LdbcIndexer.waitForIndexesToBeOnline( db );
            registerShutdownHook( managementService );

            GraphMetadataProxy metadata = GraphMetadataProxy.loadFrom( db );
            if ( !metadata.timestampResolution().equals( LdbcDateCodec.Resolution.NOT_APPLICABLE ) ||
                 !metadata.neo4jSchema().equals( Neo4jSchema.NEO4J_REGULAR ) )
            {
                throw new DbException( format( "Incompatible schema\n%s", metadata.toString() ) );
            }
            LOG.debug( metadata.toString() );

            dbConnectionState = new Neo4jConnectionState( managementService, db,
                    uri,
                    authToken,
                    loggingService,
                    annotatedQueries,
                    metadata.dateFormat(),
                    metadata.timestampResolution()
            );
        }
        else
        {
            dbConnectionState = new Neo4jConnectionState( null, null,
                    uri,
                    authToken,
                    loggingService,
                    annotatedQueries,
                    // TODO this is an assumption, read from database later, using remote cypher call
                    LdbcDateCodec.Format.NUMBER_UTC,
                    // TODO this is an assumption, read from database later, using remote cypher call
                    LdbcDateCodec.Resolution.NOT_APPLICABLE
            );
        }
    }

    @Override
    public DbConnectionState getConnectionState() throws DbException
    {
        return dbConnectionState;
    }

    @Override
    public void close() throws IOException
    {
        dbConnectionState.close();
    }

    @Override
    public void registerHandlersWithDb( Db db ) throws DbException
    {
        db.registerOperationHandler( LdbcQuery1.class, LdbcQuery1HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery2.class, LdbcQuery2HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery3.class, LdbcQuery3HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery4.class, LdbcQuery4HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery5.class, LdbcQuery5HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery6.class, LdbcQuery6HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery7.class, LdbcQuery7HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery8.class, LdbcQuery8HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery9.class, LdbcQuery9HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery10.class, LdbcQuery10HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery11.class, LdbcQuery11HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery12.class, LdbcQuery12HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery13.class, LdbcQuery13HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcQuery14.class, LdbcQuery14HandlerRemoteCypher.class );

        db.registerOperationHandler( LdbcShortQuery1PersonProfile.class, LdbcShortQuery1HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcShortQuery2PersonPosts.class, LdbcShortQuery2HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcShortQuery3PersonFriends.class, LdbcShortQuery3HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcShortQuery4MessageContent.class, LdbcShortQuery4HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcShortQuery5MessageCreator.class, LdbcShortQuery5HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcShortQuery6MessageForum.class, LdbcShortQuery6HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcShortQuery7MessageReplies.class, LdbcShortQuery7HandlerRemoteCypher.class );

        db.registerOperationHandler( LdbcUpdate1AddPerson.class, LdbcUpdate1HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcUpdate2AddPostLike.class, LdbcUpdate2HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcUpdate3AddCommentLike.class, LdbcUpdate3HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcUpdate4AddForum.class, LdbcUpdate4HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcUpdate5AddForumMembership.class, LdbcUpdate5HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcUpdate6AddPost.class, LdbcUpdate6HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcUpdate7AddComment.class, LdbcUpdate7HandlerRemoteCypher.class );
        db.registerOperationHandler( LdbcUpdate8AddFriendship.class, LdbcUpdate8HandlerRemoteCypher.class );
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
