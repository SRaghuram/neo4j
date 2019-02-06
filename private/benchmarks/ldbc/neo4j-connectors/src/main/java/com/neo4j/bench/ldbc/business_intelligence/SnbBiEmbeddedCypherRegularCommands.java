/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPerson;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedReplies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPosts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiators;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormals;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCounts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteraction;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21Zombies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialog;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinations;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopic;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery25WeightedPaths;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolution;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsers;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForums;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.Neo4jDbCommands;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery10HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery11HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery12HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery13HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery14HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery15HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery16HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery17HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery18HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery19HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery1HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery20HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery21HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery22HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery23HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery24HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery25HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery2HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery3HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery4HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery5HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery6HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery7HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery8HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular.LdbcSnbBiQuery9HandlerEmbeddedCypher;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import com.neo4j.bench.ldbc.operators.Warmup;
import com.neo4j.bench.ldbc.utils.AnnotatedQueries;
import com.neo4j.bench.ldbc.utils.AnnotatedQuery;
import com.neo4j.bench.ldbc.utils.LdbcCompilationTimeEventListener;
import com.neo4j.bench.ldbc.utils.PlanMeta;
import com.neo4j.bench.ldbc.utils.PlansSerializer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.String.format;

public class SnbBiEmbeddedCypherRegularCommands implements Neo4jDbCommands
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

    public SnbBiEmbeddedCypherRegularCommands(
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

    public static <O extends Operation<R>, R> R execute(
            Neo4jConnectionState connection,
            O operation,
            Function<Result,R> resultTransformer ) throws DbException
    {
        return execute( connection, operation, resultTransformer, operation.parameterMap() );
    }

    public static <O extends Operation<R>, R> R execute(
            Neo4jConnectionState connection,
            O operation,
            Function<Result,R> resultTransformer,
            Map<String,Object> params ) throws DbException
    {
        if ( connection.isFirstForType( operation.type() ) )
        {
            return connection.reportPlanStats(
                    operation,
                    connection.queries().queryFor( operation ),
                    params,
                    resultTransformer );
        }
        else
        {
            Result result = connection.db().execute(
                    connection.queries().queryFor( operation ).queryString(),
                    params );
            return resultTransformer.apply( result );
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
        db.registerOperationHandler(
                LdbcSnbBiQuery1PostingSummary.class,
                LdbcSnbBiQuery1HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery2TopTags.class,
                LdbcSnbBiQuery2HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery3TagEvolution.class,
                LdbcSnbBiQuery3HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery4PopularCountryTopics.class,
                LdbcSnbBiQuery4HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery5TopCountryPosters.class,
                LdbcSnbBiQuery5HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery6ActivePosters.class,
                LdbcSnbBiQuery6HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery7AuthoritativeUsers.class,
                LdbcSnbBiQuery7HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery8RelatedTopics.class,
                LdbcSnbBiQuery8HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery9RelatedForums.class,
                LdbcSnbBiQuery9HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery10TagPerson.class,
                LdbcSnbBiQuery10HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery11UnrelatedReplies.class,
                LdbcSnbBiQuery11HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery12TrendingPosts.class,
                LdbcSnbBiQuery12HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery13PopularMonthlyTags.class,
                LdbcSnbBiQuery13HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery14TopThreadInitiators.class,
                LdbcSnbBiQuery14HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery15SocialNormals.class,
                LdbcSnbBiQuery15HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery16ExpertsInSocialCircle.class,
                LdbcSnbBiQuery16HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery17FriendshipTriangles.class,
                LdbcSnbBiQuery17HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery18PersonPostCounts.class,
                LdbcSnbBiQuery18HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery19StrangerInteraction.class,
                LdbcSnbBiQuery19HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery20HighLevelTopics.class,
                LdbcSnbBiQuery20HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery21Zombies.class,
                LdbcSnbBiQuery21HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery22InternationalDialog.class,
                LdbcSnbBiQuery22HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery23HolidayDestinations.class,
                LdbcSnbBiQuery23HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery24MessagesByTopic.class,
                LdbcSnbBiQuery24HandlerEmbeddedCypher.class );
        db.registerOperationHandler(
                LdbcSnbBiQuery25WeightedPaths.class,
                LdbcSnbBiQuery25HandlerEmbeddedCypher.class );
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        Runtime.getRuntime().addShutdownHook( new Thread( graphDb::shutdown ) );
    }
}
