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

package com.neo4j.bench.ldbc.interactive;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple3;
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
import com.neo4j.bench.ldbc.utils.AnnotatedQuery;
import com.neo4j.bench.ldbc.utils.LdbcCompilationTimeEventListener;
import com.neo4j.bench.ldbc.utils.PlanMeta;
import com.neo4j.bench.ldbc.utils.PlansSerializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.String.format;

public class SnbInteractiveEmbeddedCypherRegularCommands implements Neo4jDbCommands
{
    public static final String PLANS_FILE_NAME = "-plans.json";
    private final File dbDir;
    private final File configFile;
    private final File resultsDir;
    private final String benchmarkName;
    private final LdbcCompilationTimeEventListener ldbcCompilationTimeEventListener;
    private final LoggingService loggingService;
    private Neo4jConnectionState connection;
    private final AnnotatedQueries annotatedQueries;
    private final boolean doWarmup;

    public SnbInteractiveEmbeddedCypherRegularCommands(
            File dbDir,
            File configFile,
            File resultsDir,
            String benchmarkName,
            LoggingService loggingService,
            AnnotatedQueries annotatedQueries,
            boolean doWarmup )
    {
        this.dbDir = dbDir;
        this.configFile = configFile;
        this.resultsDir = resultsDir;
        this.benchmarkName = benchmarkName;
        this.loggingService = loggingService;
        this.ldbcCompilationTimeEventListener = new LdbcCompilationTimeEventListener();
        this.annotatedQueries = annotatedQueries;
        this.doWarmup = doWarmup;
    }

    @Override
    public void init() throws DbException
    {
        GraphDatabaseService db = Neo4jDb.newDb( dbDir, configFile );
        LdbcIndexer.waitForIndexesToBeOnline( db );
        registerShutdownHook( db );

        LdbcCompilationTimeEventListener ldbcCompilationTimeEventListener = new LdbcCompilationTimeEventListener();
        Monitors monitors = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Monitors.class );
        monitors.addMonitorListener( ldbcCompilationTimeEventListener );

        GraphMetadataProxy metadata = GraphMetadataProxy.loadFrom( db );
        if ( !metadata.timestampResolution().equals( LdbcDateCodec.Resolution.NOT_APPLICABLE ) ||
             !metadata.neo4jSchema().equals( Neo4jSchema.NEO4J_REGULAR ) )
        {
            throw new DbException( format( "Incompatible schema\n%s", metadata.toString() ) );
        }
        loggingService.info( metadata.toString() );

        connection = new Neo4jConnectionState(
                db,
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
        try
        {
            connection.close();
            com.ldbc.driver.util.FileUtils.assertDirectoryExists( resultsDir );
            String plansFilename = benchmarkName + PLANS_FILE_NAME;
            File plansFile = new File( resultsDir, plansFilename );
            com.ldbc.driver.util.FileUtils.assertFileDoesNotExist( plansFile );
            com.ldbc.driver.util.FileUtils.tryCreateFile( plansFile, true );
            serializePlansAndWriteToFile( plansFile );
        }
        catch ( Exception e )
        {
            throw new IOException( "Error writing plans file", e );
        }
    }

    private void serializePlansAndWriteToFile( File file ) throws DbException
    {
        try ( PrintStream out = new PrintStream( new FileOutputStream( file ) ) )
        {
            PlansSerializer plansSerializer = new PlansSerializer( out );

            for ( AnnotatedQuery annotatedQuery : annotatedQueries.allQueries() )
            {
                if ( connection.hasPlanStatsFor( annotatedQuery.operationType() ) )
                {
                    setCompilationTimesAsUndefined(
                            connection.planMetaFor( annotatedQuery.operationType() ),
                            annotatedQuery.queryString()
                    );
                }
            }

        List<Tuple3<String,ExecutionPlanDescription,PlanMeta>> plans = new ArrayList<>();
            for ( AnnotatedQuery annotatedQuery : annotatedQueries.allQueries() )
            {
                if ( connection.hasPlanStatsFor( annotatedQuery.operationType() ) )
                {
                    plans.add(
                            Tuple.tuple3(
                                    annotatedQuery.operationDescription(),
                                    connection.planDescriptionFor( annotatedQuery.operationType() ),
                                    connection.planMetaFor( annotatedQuery.operationType() )
                            )
                    );
                }
            }

            plansSerializer.serializePlansToJson( plans );
            out.flush();
            out.close();
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Error writing query plans to file\nFile: %s", file.getAbsolutePath() ), e );
        }
    }

    private void setCompilationTimesAsUndefined(
            PlanMeta planMeta,
            String queryString )
    {
        setCompilationTimes(
                planMeta,
                ldbcCompilationTimeEventListener.getTotalTime( queryString ),
                ldbcCompilationTimeEventListener.getParsingTimeElapsed( queryString ),
                ldbcCompilationTimeEventListener.getAstRewritingTimeElapsed( queryString ),
                ldbcCompilationTimeEventListener.getSemanticCheckTimeElapsed( queryString ),
                ldbcCompilationTimeEventListener.getLogicalPlanTimeElapsed( queryString ),
                ldbcCompilationTimeEventListener.getExecutionPlanTimeElapsed( queryString )
        );
        planMeta.setQuery( queryString );
    }

    private void setCompilationTimes(
            PlanMeta planMeta,
            long totalTime,
            long parsingTime,
            long rewritingTime,
            long semanticCheckTime,
            long planningTime,
            long executionPlanBuildingTime )
    {
        planMeta.setTotalTime( totalTime );
        planMeta.setParsingTime( parsingTime );
        planMeta.setRewritingTime( rewritingTime );
        planMeta.setSemanticCheckTime( semanticCheckTime );
        planMeta.setPlanningTime( planningTime );
        planMeta.setExecutionPlanBuildingTime( executionPlanBuildingTime );
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

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        Runtime.getRuntime().addShutdownHook( new Thread( graphDb::shutdown ) );
    }
}
