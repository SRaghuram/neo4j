/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.queries.annotation.AttachMetricsAnnotation;
import com.neo4j.bench.client.queries.annotation.AttachTestRunAnnotation;
import com.neo4j.bench.client.queries.annotation.DeleteAnnotation;
import com.neo4j.bench.client.queries.regression.AttachRegression;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.client.queries.schema.SetStoreVersion;
import com.neo4j.bench.client.queries.schema.VerifyStoreSchema;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.model.Annotation;
import com.neo4j.bench.model.model.AuxiliaryMetrics;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkPlan;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.model.PlanTree;
import com.neo4j.bench.model.model.Project;
import com.neo4j.bench.model.model.Regression;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunError;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.util.JsonUtil;
import com.neo4j.harness.junit.extension.EnterpriseNeo4jExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.client.queries.schema.VerifyStoreSchema.patternCountInStore;
import static com.neo4j.bench.common.results.ErrorReportingPolicy.FAIL;
import static com.neo4j.bench.common.results.ErrorReportingPolicy.IGNORE;
import static com.neo4j.bench.common.results.ErrorReportingPolicy.REPORT_THEN_FAIL;
import static com.neo4j.bench.model.options.Edition.COMMUNITY;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestDirectoryExtension
public class SubmitTestRunsAndPlansIT
{
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier( false );

    @RegisterExtension
    static final Neo4jExtension neo4jExtension = EnterpriseNeo4jExtension.builder()
                                                                         .withConfig( GraphDatabaseSettings.auth_enabled, false )
                                                                         .withConfig( BoltConnector.enabled, true )
                                                                         .withConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.OPTIONAL )
                                                                         .build();

    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    @Inject
    public TestDirectory temporaryFolder;

    private URI boltUri;

    @BeforeEach
    void setUp( GraphDatabaseService databaseService )
    {
        HostnamePort address = ((GraphDatabaseAPI) databaseService).getDependencyResolver()
                                                                   .resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        boltUri = URI.create( "bolt://" + address.toString() );
    }

    @AfterEach
    void cleanUpDb( GraphDatabaseService databaseService )
    {
        // this is hacky HACK, needs to be fixed in Neo4jExtension
        try ( Transaction transaction = databaseService.beginTx() )
        {
            transaction.execute( "MATCH (n) DETACH DELETE n" ).close();
            transaction.commit();
        }
    }

    @Test
    void shouldNotCorruptSchemaWhenCallingSetVersionMultipleTimes()
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new SetStoreVersion( StoreClient.VERSION ), 1 );
            QUERY_RETRIER.execute( client, new SetStoreVersion( StoreClient.VERSION ), 1 );
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
        }
    }

    @Test
    void shouldRespectErrorReportingPolicy() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema(), 1 );
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            /*
            Should submit results successfully, regardless of error policy, when no errors are present
             */

            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );
            File testRunResultsJson1 = temporaryFolder.file( "results1.json" ).toFile();
            TestRunReport testRunReport = createTestRunReportTwoProjects(
                    testRun1,
                    newArrayList(), // no plans
                    newArrayList(), // no errors
                    group,
                    benchmark1, benchmark2 );
            JsonUtil.serializeJson( testRunResultsJson1.toPath(), testRunReport );
            // no exception is expected
            Main.main( new String[]{
                    "report",
                    ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson1.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, FAIL.name()} );

            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "AuxiliaryMetrics", 2, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "Instance", 1, client );
            assertRelationship( "Environment", "HAS_INSTANCE", "Instance", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "PlanTree", 0, client );
            assertLabelCount( "Operator", 0, client );
            // Annotation specific
            assertLabelCount( "Annotation", 0, client );

            /*
            Should fail to submit results when errors are present and error policy is FAIL
             */

            TestRun testRun2 = new TestRun( "id2", 1, 1, 1, 1, "user" );
            final File testRunResultsJson2 = temporaryFolder.file( "results2.json" ).toFile();
            testRunReport = createTestRunReportTwoProjects(
                    testRun2,
                    newArrayList(), // no plans
                    newArrayList( new TestRunError( "group1", "benchmark1", "description 1" ),
                                  new TestRunError( "group2", "benchmark2", "description 2" ) ), // has errors
                    group,
                    benchmark1, benchmark2 );
            JsonUtil.serializeJson( testRunResultsJson2.toPath(), testRunReport );
            BenchmarkUtil.assertException( RuntimeException.class,
                                           () ->
                                                   Main.main( new String[]{
                                                           "report",
                                                           ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                                                           ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                                                           ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                                                           ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson2.getAbsolutePath(),
                                                           ReportCommand.CMD_ERROR_POLICY, FAIL.name()} ) );

            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "AuxiliaryMetrics", 2, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "Instance", 1, client );
            assertRelationship( "Environment", "HAS_INSTANCE", "Instance", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "PlanTree", 0, client );
            assertLabelCount( "Operator", 0, client );
            // Annotation specific
            assertLabelCount( "Annotation", 0, client );

            /*
            Should successfully submit results, and then throw exception, when policy is REPORT_THEN_FAIL
             */

            BenchmarkUtil.assertException( RuntimeException.class,
                                           () ->
                                                   Main.main( new String[]{
                                                           "report",
                                                           ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                                                           ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                                                           ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                                                           ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson2.getAbsolutePath(),
                                                           ReportCommand.CMD_ERROR_POLICY, REPORT_THEN_FAIL.name()} ) );

            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 2, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 4, client );
            assertLabelCount( "AuxiliaryMetrics", 4, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 6, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 2, client );
            assertLabelCount( "Instance", 1, client );
            assertRelationship( "Environment", "HAS_INSTANCE", "Instance", 2, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "PlanTree", 0, client );
            assertLabelCount( "Operator", 0, client );
            // Annotation specific
            assertLabelCount( "Annotation", 0, client );

            /*
            Should successfully submit results, and not throw exception, when policy is IGNORE
             */

            TestRun testRun3 = new TestRun( "id3", 1, 1, 1, 1, "user" );
            File testRunResultsJson3 = temporaryFolder.file( "results3.json" ).toFile();
            testRunReport = createTestRunReportTwoProjects(
                    testRun3,
                    newArrayList(), // no plans
                    newArrayList( new TestRunError( "group1", "benchmark1", "description 1" ),
                                  new TestRunError( "group2", "benchmark2", "description 2" ) ), // has errors
                    group,
                    benchmark1, benchmark2 );
            JsonUtil.serializeJson( testRunResultsJson3.toPath(), testRunReport );
            // no exception is expected
            Main.main( new String[]{
                    "report",
                    ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson3.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, IGNORE.name()} );

            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 3, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 6, client );
            assertLabelCount( "AuxiliaryMetrics", 6, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 9, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 3, client );
            assertLabelCount( "Instance", 1, client );
            assertRelationship( "Environment", "HAS_INSTANCE", "Instance", 3, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "PlanTree", 0, client );
            assertLabelCount( "Operator", 0, client );
            // Annotation specific
            assertLabelCount( "Annotation", 0, client );
        }
    }

    private void assertRelationship( String from, String relationship, String to, int relationshipCount, StoreClient client )
    {
        try ( Session session = client.session() )
        {
            Result result = session.run( format( "MATCH p=(from:%s)-[:%s]->(to:%s) return p", from, relationship, to ) );
            assertEquals( relationshipCount, result.list().size() );
        }
    }

    @Test
    void shouldMaintainSchemaConsistency() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema(), 1 );
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark3 = Benchmark.benchmarkFor( "desc3", "bench3", Benchmark.Mode.LATENCY, emptyMap(), "RETURN 1" );

            ArrayList<BenchmarkPlan> benchmarkPlans1 = newArrayList(
                    new BenchmarkPlan( group, benchmark1, plan( "a" ) ),
                    new BenchmarkPlan( group, benchmark2, plan( "a" ) ) );

            File testRunResultsJson1 = temporaryFolder.file( "results1.json" ).toFile();
            ArrayList<TestRunError> errors = newArrayList();
            TestRunReport testRunReport1 = createTestRunReportTwoProjects(
                    testRun1,
                    benchmarkPlans1,
                    errors,
                    group,
                    benchmark1, benchmark2, benchmark3 );
            JsonUtil.serializeJson(
                    testRunResultsJson1.toPath(), testRunReport1 );
            File profileDir = createProfileFiles( temporaryFolder, group, benchmark1, benchmark2 );
            Main.main( new String[]{
                    "add-profiles",
                    AddProfilesCommand.CMD_DIR, profileDir.getAbsolutePath(),
                    AddProfilesCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson1.getAbsolutePath(),
                    AddProfilesCommand.CMD_S3_BUCKET, "some-s3-bucket"} );
            Main.main( new String[]{
                    "report",
                    ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson1.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            Annotation annotation1 = new Annotation( "comment1", 1, "author1" );
            QUERY_RETRIER
                    .execute( client, new AttachTestRunAnnotation( testRunReport1.testRun().id(), annotation1 ), 1 );

            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 3, client );
            assertLabelCount( "Metrics", 3, client );
            assertLabelCount( "AuxiliaryMetrics", 3, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 3, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 4, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "PlanTree", 1, client );
            assertLabelCount( "Operator", 7, client );
            // Annotation specific
            assertLabelCount( "Annotation", 1, client );

            TestRun testRun2 = new TestRun( "id2", 1, 1, 2, 1, "user" );
            Benchmark benchmark4 = Benchmark.benchmarkFor( "desc4", "bench4", Benchmark.Mode.LATENCY, emptyMap() );

            List<BenchmarkPlan> benchmarkPlans2 = newArrayList(
                    new BenchmarkPlan( group, benchmark1, plan( "a" ) ),
                    new BenchmarkPlan( group, benchmark2, plan( "b" ) ),
                    new BenchmarkPlan( group, benchmark4, plan( "c" ) ) );
            File testRunResultsJson2 = temporaryFolder.file( "results2.json" ).toFile();
            TestRunReport testRunReport2 = createTestRunReport(
                    testRun2,
                    benchmarkPlans2,
                    errors,
                    group,
                    benchmark1, benchmark2, benchmark4 );
            JsonUtil.serializeJson( testRunResultsJson2.toPath(), testRunReport2 );
            Main.main( new String[]{
                    "report",
                    ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson2.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            int addedAnnotations = 0;
            for ( BenchmarkGroupBenchmark bgb : testRunReport2.benchmarkGroupBenchmarks() )
            {
                addedAnnotations++;
                QUERY_RETRIER.execute( client,
                                       new AttachMetricsAnnotation( testRunReport2.testRun(),
                                                                    bgb.benchmark(),
                                                                    bgb.benchmarkGroup(),
                                                                    new Annotation( "comment", 1, "author" ) ),
                                       1 );
            }
            int addedRegressions = 0;
            for ( BenchmarkGroupBenchmark bgb : testRunReport2.benchmarkGroupBenchmarks() )
            {
                addedRegressions++;
                QUERY_RETRIER.execute( client,
                                       new AttachRegression( testRunReport2.testRun(),
                                                             bgb.benchmark(),
                                                             bgb.benchmarkGroup(),
                                                             createRegression() ),
                                       1 );
            }

            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 2, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 4, client );
            assertLabelCount( "Metrics", 6, client );
            assertLabelCount( "AuxiliaryMetrics", 6, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 4, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 8, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 2, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 5, client );
            assertLabelCount( "PlanTree", 3, client );
            assertLabelCount( "Operator", 21, client );
            // Annotation specific
            assertLabelCount( "Annotation", 1 + addedAnnotations, client );
            // Regression specific
            assertLabelCount( "Regression", addedRegressions, client );

            QUERY_RETRIER.execute( client, new DeleteAnnotation( annotation1 ), 1 );

            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 2, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 4, client );
            assertLabelCount( "Metrics", 6, client );
            assertLabelCount( "AuxiliaryMetrics", 6, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 4, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 8, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 2, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 5, client );
            assertLabelCount( "PlanTree", 3, client );
            assertLabelCount( "Operator", 21, client );
            // Annotation specific
            assertLabelCount( "Annotation", addedAnnotations, client );
        }
    }

    private Regression createRegression() throws MalformedURLException
    {
        return Regression.create( "comment",
                                  "author",
                                  Regression.Status.REPORTED,
                                  new URL( "http://some-domain/trello-card" ),
                                  "4.1" );
    }

    @Test
    void shouldCreateNewPlansWhenNecessary() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD, 1 ) )
        {
            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );

            List<BenchmarkPlan> benchmarkPlans = newArrayList(
                    new BenchmarkPlan( group, benchmark1, plan( "a" ) ),
                    new BenchmarkPlan( group, benchmark2, plan( "b" ) ) );
            File testRunResultsJson = temporaryFolder.file( "results.json" ).toFile();
            ArrayList<TestRunError> errors = newArrayList( new TestRunError( "group1", "benchmark1", "description 1" ),
                                                           new TestRunError( "group2", "benchmark2", "description 2" ) );
            TestRunReport testRunReport1 =
                    createTestRunReport( testRun1, benchmarkPlans, errors, group, benchmark1, benchmark2 );
            JsonUtil.serializeJson( testRunResultsJson.toPath(), testRunReport1 );
            File profileDir = createProfileFiles( temporaryFolder, group, benchmark1, benchmark2 );
            Main.main( new String[]{
                    "add-profiles",
                    AddProfilesCommand.CMD_DIR, profileDir.getAbsolutePath(),
                    AddProfilesCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson.getAbsolutePath(),
                    AddProfilesCommand.CMD_S3_BUCKET, "some-s3-bucket"} );
            Main.main( new String[]{
                    "report",
                    ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            Annotation annotation1 = new Annotation( "comment1", 1, "author1" );
            QUERY_RETRIER
                    .execute( client, new AttachTestRunAnnotation( testRunReport1.testRun().id(), annotation1 ), 1 );

            int addedAnnotations = 0;
            for ( BenchmarkGroupBenchmark bgb : testRunReport1.benchmarkGroupBenchmarks() )
            {
                addedAnnotations++;
                QUERY_RETRIER.execute( client,
                                       new AttachMetricsAnnotation( testRunReport1.testRun(),
                                                                    bgb.benchmark(),
                                                                    bgb.benchmarkGroup(),
                                                                    new Annotation( "comment", 1, "author" ) ),
                                       1 );
            }

            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "AuxiliaryMetrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "PlanTree", 2, client );
            assertLabelCount( "Operator", 14, client );
            // Annotation specific
            assertLabelCount( "Annotation", 1 + addedAnnotations, client );

            QUERY_RETRIER.execute( client, new DeleteAnnotation( annotation1 ), 1 );

            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "AuxiliaryMetrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "PlanTree", 2, client );
            assertLabelCount( "Operator", 14, client );
            // Annotation specific
            assertLabelCount( "Annotation", addedAnnotations, client );
        }
    }

    @Test
    void shouldReusePlansWhenPossible() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD, 1 ) )
        {
            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );

            List<BenchmarkPlan> benchmarkPlans = newArrayList(
                    new BenchmarkPlan( group, benchmark1, plan( "a" ) ),
                    new BenchmarkPlan( group, benchmark2, plan( "a" ) ) );
            File testRunResultsJson = temporaryFolder.file( "results.json" ).toFile();
            ArrayList<TestRunError> errors = newArrayList( new TestRunError( "group1", "benchmark1", "description 1" ),
                                                           new TestRunError( "group2", "benchmark2", "description 2" ) );
            TestRunReport testRunReport1 =
                    createTestRunReport( testRun1, benchmarkPlans, errors, group, benchmark1, benchmark2 );
            JsonUtil.serializeJson( testRunResultsJson.toPath(), testRunReport1 );
            File profileDir = createProfileFiles( temporaryFolder, group, benchmark1, benchmark2 );
            Main.main( new String[]{
                    "add-profiles",
                    AddProfilesCommand.CMD_DIR, profileDir.getAbsolutePath(),
                    AddProfilesCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson.getAbsolutePath(),
                    AddProfilesCommand.CMD_S3_BUCKET, "some-s3-bucket"} );
            Main.main( new String[]{
                    "report",
                    ReportCommand.CMD_RESULTS_STORE_URI, boltUri.toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            Annotation annotation1 = new Annotation( "comment1", 1, "author1" );
            QUERY_RETRIER
                    .execute( client, new AttachTestRunAnnotation( testRunReport1.testRun().id(), annotation1 ), 1 );

            int addedAnnotations = 0;
            for ( BenchmarkGroupBenchmark bgb : testRunReport1.benchmarkGroupBenchmarks() )
            {
                addedAnnotations++;
                QUERY_RETRIER.execute( client,
                                       new AttachMetricsAnnotation( testRunReport1.testRun(),
                                                                    bgb.benchmark(),
                                                                    bgb.benchmarkGroup(),
                                                                    new Annotation( "comment", 1, "author" ) ),
                                       1 );
            }

            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "AuxiliaryMetrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "PlanTree", 1, client );
            assertLabelCount( "Operator", 7, client );
            // Annotation specific
            assertLabelCount( "Annotation", 1 + addedAnnotations, client );

            QUERY_RETRIER.execute( client, new DeleteAnnotation( annotation1 ), 1 );

            // general
            QUERY_RETRIER.execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "AuxiliaryMetrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "PlanTree", 1, client );
            assertLabelCount( "Operator", 7, client );
            // Annotation specific
            assertLabelCount( "Annotation", addedAnnotations, client );
        }
    }

    private void assertEmptyStore( StoreClient client )
    {
        // general
        assertLabelCount( "TestRun", 0, client );
        assertLabelCount( "BenchmarkGroup", 0, client );
        assertLabelCount( "Benchmark", 0, client );
        assertLabelCount( "Metrics", 0, client );
        assertLabelCount( "Profiles", 0, client );
        assertLabelCount( "BenchmarkParams", 0, client );
        assertLabelCount( "Project", 0, client );
        assertLabelCount( "Neo4jConfig", 0, client );
        assertLabelCount( "Java", 0, client );
        assertLabelCount( "Environment", 0, client );
        assertLabelCount( "BenchmarkTool", 0, client );
        // plan specific
        assertLabelCount( "Plan", 0, client );
        assertLabelCount( "PlanTree", 0, client );
        assertLabelCount( "Operator", 0, client );
        // Annotation specific
        assertLabelCount( "Annotation", 0, client );
    }

    private static File createProfileFiles(
            TestDirectory temporaryFolder,
            BenchmarkGroup group,
            Benchmark benchmark1,
            Benchmark benchmark2 ) throws IOException
    {
        Path absolutePath = temporaryFolder.absolutePath();
        Path topFolder = absolutePath.resolve( "profiles" );
        Files.createDirectories( topFolder );
        Files.createFile( absolutePath.resolve( "archive.tar.gz" ) );

        Files.createFile( absolutePath.resolve( "profiles/" + group.name() + "." + benchmark1.name() + ".jfr" ) );
        Files.createFile( absolutePath.resolve( "profiles/" + group.name() + "." + benchmark1.name() + "-jfr.svg" ) );
        Files.createFile( absolutePath.resolve( "profiles/" + group.name() + "." + benchmark1.name() + ".async" ) );
        Files.createFile( absolutePath.resolve( "profiles/" + group.name() + "." + benchmark1.name() + "-async.svg" ) );

        Files.createFile( absolutePath.resolve( "profiles/" + group.name() + "." + benchmark2.name() + ".jfr" ) );
        Files.createFile( absolutePath.resolve( "profiles/" + group.name() + "." + benchmark2.name() + "-jfr.svg" ) );
        return topFolder.toFile();
    }

    private void assertLabelCount( String label, int expectedCount, StoreClient client )
    {
        try ( Session session = client.session() )
        {
            assertThat( format( "has correct number of :%s nodes", label ),
                        patternCountInStore( format( "(:%s)", label ), session ),
                        equalTo( expectedCount ) );
        }
    }

    private TestRunReport createTestRunReport(
            TestRun testRun,
            List<BenchmarkPlan> benchmarkPlans,
            List<TestRunError> errors,
            BenchmarkGroup benchmarkGroup,
            Benchmark... benchmarks )
    {
        Metrics metrics = getMetrics();
        AuxiliaryMetrics auxiliaryMetrics = getAuxiliaryMetrics();
        Neo4jConfig neo4jConfig = Neo4jConfig.empty();
        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        Stream.of( benchmarks ).forEach( benchmark -> benchmarkGroupBenchmarkMetrics.add(
                benchmarkGroup,
                benchmark,
                metrics,
                auxiliaryMetrics,
                neo4jConfig ) );
        return new TestRunReport(
                testRun,
                new BenchmarkConfig( emptyMap() ),
                Sets.newHashSet( new Project( Repository.NEO4J, "commit", "3.2.1", COMMUNITY, "branch", "owner" ) ),
                neo4jConfig,
                Environment.local(),
                benchmarkGroupBenchmarkMetrics,
                new BenchmarkTool( Repository.LDBC_BENCH, "commit", "neo-technology", "3.2" ),
                Java.current( "args" ),
                benchmarkPlans,
                errors );
    }

    private TestRunReport createTestRunReportTwoProjects(
            TestRun testRun,
            List<BenchmarkPlan> benchmarkPlans,
            List<TestRunError> errors,
            BenchmarkGroup benchmarkGroup,
            Benchmark... benchmarks )
    {
        Metrics metrics = getMetrics();
        AuxiliaryMetrics auxiliaryMetrics = getAuxiliaryMetrics();
        Neo4jConfig neo4jConfig = Neo4jConfig.empty();
        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        Stream.of( benchmarks ).forEach( benchmark -> benchmarkGroupBenchmarkMetrics.add(
                benchmarkGroup,
                benchmark,
                metrics,
                auxiliaryMetrics,
                neo4jConfig ) );
        return new TestRunReport(
                testRun,
                new BenchmarkConfig( emptyMap() ),
                Sets.newHashSet( new Project( Repository.NEO4J, "commit", "3.2.1", COMMUNITY, "branch", "owner" ),
                                 new Project( Repository.CAPS, "commit", "3.2.1", COMMUNITY, "branch", "owner" ) ),
                neo4jConfig,
                Environment.local(),
                benchmarkGroupBenchmarkMetrics,
                new BenchmarkTool( Repository.LDBC_BENCH, "commit", "neo-technology", "3.2" ),
                Java.current( "args" ),
                benchmarkPlans,
                errors );
    }

    public static Plan plan( String description )
    {
        PlanOperator leftLeaf1 = new PlanOperator( 0, "left-leaf", 1, 2.0, 3 );
        leftLeaf1.addArgument( "a", "b" );
        PlanOperator leftLeaf2 = new PlanOperator( 1, "left-leaf", 1, 2.0, 3 );
        leftLeaf1.addArgument( "a", "b" );

        PlanOperator rightLeaf1 = new PlanOperator( 2, "right-leaf-1", 2, 3.0, 4 );
        rightLeaf1.addArgument( "a", "7" );
        rightLeaf1.addArgument( "b", "42" );
        PlanOperator rightLeaf2 = new PlanOperator( 3, "right-leaf-2", 3, 4.0, 5 );
        rightLeaf2.addArgument( "c", "pies" );

        PlanOperator left = new PlanOperator( 4, "left", 1, 1.0, 1 );
        left.addChild( leftLeaf1 );
        left.addChild( leftLeaf2 );

        PlanOperator right = new PlanOperator( 5, "right", 1, 1.0, 1 );
        right.addArgument( "cakes", "not as good as pies" );
        right.addChild( rightLeaf1 );
        right.addChild( rightLeaf2 );

        PlanOperator root = new PlanOperator( 6, "root", 0, 0.0, 0 );
        root.addArgument( "knock_knock", "who is there?" );
        root.addChild( left );
        root.addChild( right );

        return new Plan(
                "cost",
                "cost",
                "cost",
                "slotted",
                "slotted",
                "slotted",
                "3.2",
                new PlanTree( description, root )
        );
    }

    private Metrics getMetrics()
    {
        return new Metrics( MILLISECONDS, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 );
    }

    private AuxiliaryMetrics getAuxiliaryMetrics()
    {
        return new AuxiliaryMetrics( "rows", 1, 10, 5.0, 42, 2.5, 5.0, 7.5, 9.0, 9.5, 9.9, 9.99 );
    }
}
