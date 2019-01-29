package com.neo4j.bench.client;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.model.Annotation;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkConfig;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkPlan;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.Environment;
import com.neo4j.bench.client.model.Java;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Plan;
import com.neo4j.bench.client.model.PlanCompilationMetrics;
import com.neo4j.bench.client.model.PlanOperator;
import com.neo4j.bench.client.model.PlanTree;
import com.neo4j.bench.client.model.Project;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunError;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.queries.AttachMetricsAnnotation;
import com.neo4j.bench.client.queries.AttachTestRunAnnotation;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.DeleteAnnotation;
import com.neo4j.bench.client.queries.DropSchema;
import com.neo4j.bench.client.queries.SetStoreVersion;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.client.ReportCommand.ErrorReportingPolicy.FAIL;
import static com.neo4j.bench.client.ReportCommand.ErrorReportingPolicy.IGNORE;
import static com.neo4j.bench.client.ReportCommand.ErrorReportingPolicy.REPORT_THEN_FAIL;
import static com.neo4j.bench.client.model.Edition.COMMUNITY;
import static com.neo4j.bench.client.queries.VerifyStoreSchema.patternCountInStore;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SubmitTestRunsAndPlansIT
{
    private final Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( temporaryFolder ).around( neo4j );
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotCorruptSchemaWhenCallingSetVersionMultipleTimes() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            new QueryRetrier().execute( client, new SetStoreVersion( StoreClient.VERSION ), 1 );
            new QueryRetrier().execute( client, new SetStoreVersion( StoreClient.VERSION ), 1 );
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
        }
    }

    @Test
    public void shouldRespectErrorReportingPolicy() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            new QueryRetrier().execute( client, new DropSchema(), 1 );
            new QueryRetrier().execute( client, new CreateSchema(), 1 );
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            /*
            Should submit results successfully, regardless of error policy, when no errors are present
             */

            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );
            File testRunResultsJson1 = temporaryFolder.newFile();
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
                    ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson1.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, FAIL.name()} );

            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "CompilationMetrics", 0, client );
            assertLabelCount( "PlanTree", 0, client );
            assertLabelCount( "Operator", 0, client );
            // Annotation specific
            assertLabelCount( "Annotation", 0, client );

            /*
            Should fail to submit results when errors are present and error policy is FAIL
             */

            TestRun testRun2 = new TestRun( "id2", 1, 1, 1, 1, "user" );
            final File testRunResultsJson2 = temporaryFolder.newFile();
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
                                                           ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                                                           ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                                                           ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                                                           ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson2.getAbsolutePath(),
                                                           ReportCommand.CMD_ERROR_POLICY, FAIL.name()} ) );

            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "CompilationMetrics", 0, client );
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
                                                           ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                                                           ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                                                           ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                                                           ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson2.getAbsolutePath(),
                                                           ReportCommand.CMD_ERROR_POLICY, REPORT_THEN_FAIL.name()} ) );

            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 2, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 4, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 6, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "CompilationMetrics", 0, client );
            assertLabelCount( "PlanTree", 0, client );
            assertLabelCount( "Operator", 0, client );
            // Annotation specific
            assertLabelCount( "Annotation", 0, client );

            /*
            Should successfully submit results, and not throw exception, when policy is IGNORE
             */

            TestRun testRun3 = new TestRun( "id3", 1, 1, 1, 1, "user" );
            File testRunResultsJson3 = temporaryFolder.newFile();
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
                    ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson3.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, IGNORE.name()} );

            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 3, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 6, client );
            assertLabelCount( "Profiles", 0, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 9, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 0, client );
            assertLabelCount( "CompilationMetrics", 0, client );
            assertLabelCount( "PlanTree", 0, client );
            assertLabelCount( "Operator", 0, client );
            // Annotation specific
            assertLabelCount( "Annotation", 0, client );
        }
    }

    @Test
    public void shouldMaintainSchemaConsistency() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            new QueryRetrier().execute( client, new DropSchema(), 1 );
            new QueryRetrier().execute( client, new CreateSchema(), 1 );
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark3 = Benchmark.benchmarkFor( "desc3", "bench3", Benchmark.Mode.LATENCY, emptyMap(),"RETURN 1" );

            ArrayList<BenchmarkPlan> benchmarkPlans1 = newArrayList(
                    new BenchmarkPlan( group, benchmark1, plan( "a" ) ),
                    new BenchmarkPlan( group, benchmark2, plan( "a" ) ) );

            File testRunResultsJson1 = temporaryFolder.newFile();
            ArrayList<TestRunError> errors = newArrayList();
            TestRunReport testRunReport1 = createTestRunReportTwoProjects(
                    testRun1,
                    benchmarkPlans1,
                    errors,
                    group,
                    benchmark1, benchmark2,benchmark3 );
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
                    ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson1.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            Annotation annotation1 = new Annotation( "comment1", 1, "author1" );
            new QueryRetrier()
                    .execute( client, new AttachTestRunAnnotation( testRunReport1.testRun().id(), annotation1 ), 1 );

            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 3, client );
            assertLabelCount( "Metrics", 3, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 3, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 4, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "CompilationMetrics", 2, client );
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
            File testRunResultsJson2 = temporaryFolder.newFile();
            TestRunReport testRunReport2 = createTestRunReport(
                    testRun2,
                    benchmarkPlans2,
                    errors,
                    group,
                    benchmark1, benchmark2, benchmark4 );
            JsonUtil.serializeJson( testRunResultsJson2.toPath(), testRunReport2 );
            Main.main( new String[]{
                    "report",
                    ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson2.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            int addedAnnotations = 0;
            for ( BenchmarkGroupBenchmark bgb : testRunReport2.benchmarkGroupBenchmarks() )
            {
                addedAnnotations++;
                new QueryRetrier().execute( client,
                                            new AttachMetricsAnnotation( testRunReport2.testRun().id(),
                                                                         bgb.benchmark().name(),
                                                                         bgb.benchmarkGroup().name(),
                                                                         new Annotation( "comment", 1, "author" ) ),
                                            1 );
            }

            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 2, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 4, client );
            assertLabelCount( "Metrics", 6, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 4, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 8, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 5, client );
            assertLabelCount( "CompilationMetrics", 5, client );
            assertLabelCount( "PlanTree", 3, client );
            assertLabelCount( "Operator", 21, client );
            // Annotation specific
            assertLabelCount( "Annotation", 1 + addedAnnotations, client );

            new QueryRetrier().execute( client, new DeleteAnnotation( annotation1 ), 1 );

            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 2, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 4, client );
            assertLabelCount( "Metrics", 6, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 4, client );
            assertLabelCount( "Project", 2, client );
            assertLabelCount( "Neo4jConfig", 8, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 5, client );
            assertLabelCount( "CompilationMetrics", 5, client );
            assertLabelCount( "PlanTree", 3, client );
            assertLabelCount( "Operator", 21, client );
            // Annotation specific
            assertLabelCount( "Annotation", addedAnnotations, client );
        }
    }

    @Test
    public void shouldCreateNewPlansWhenNecessary() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );

            List<BenchmarkPlan> benchmarkPlans = newArrayList(
                    new BenchmarkPlan( group, benchmark1, plan( "a" ) ),
                    new BenchmarkPlan( group, benchmark2, plan( "b" ) ) );
            File testRunResultsJson = temporaryFolder.newFile();
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
                    ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            Annotation annotation1 = new Annotation( "comment1", 1, "author1" );
            new QueryRetrier()
                    .execute( client, new AttachTestRunAnnotation( testRunReport1.testRun().id(), annotation1 ), 1 );

            int addedAnnotations = 0;
            for ( BenchmarkGroupBenchmark bgb : testRunReport1.benchmarkGroupBenchmarks() )
            {
                addedAnnotations++;
                new QueryRetrier().execute( client,
                                            new AttachMetricsAnnotation( testRunReport1.testRun().id(),
                                                                         bgb.benchmark().name(),
                                                                         bgb.benchmarkGroup().name(),
                                                                         new Annotation( "comment", 1, "author" ) ),
                                            1 );
            }

            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "CompilationMetrics", 2, client );
            assertLabelCount( "PlanTree", 2, client );
            assertLabelCount( "Operator", 14, client );
            // Annotation specific
            assertLabelCount( "Annotation", 1 + addedAnnotations, client );

            new QueryRetrier().execute( client, new DeleteAnnotation( annotation1 ), 1 );

            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "CompilationMetrics", 2, client );
            assertLabelCount( "PlanTree", 2, client );
            assertLabelCount( "Operator", 14, client );
            // Annotation specific
            assertLabelCount( "Annotation", addedAnnotations, client );
        }
    }

    @Test
    public void shouldReusePlansWhenPossible() throws Exception
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertEmptyStore( client );

            TestRun testRun1 = new TestRun( "id1", 1, 1, 1, 1, "user" );
            BenchmarkGroup group = new BenchmarkGroup( "group1" );
            Benchmark benchmark1 = Benchmark.benchmarkFor( "desc1", "bench1", Benchmark.Mode.LATENCY, emptyMap() );
            Benchmark benchmark2 = Benchmark.benchmarkFor( "desc2", "bench2", Benchmark.Mode.LATENCY, emptyMap() );

            List<BenchmarkPlan> benchmarkPlans = newArrayList(
                    new BenchmarkPlan( group, benchmark1, plan( "a" ) ),
                    new BenchmarkPlan( group, benchmark2, plan( "a" ) ) );
            File testRunResultsJson = temporaryFolder.newFile();
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
                    ReportCommand.CMD_RESULTS_STORE_URI, neo4j.boltURI().toString(),
                    ReportCommand.CMD_RESULTS_STORE_USER, USERNAME,
                    ReportCommand.CMD_RESULTS_STORE_PASSWORD, PASSWORD,
                    ReportCommand.CMD_TEST_RUN_RESULTS, testRunResultsJson.getAbsolutePath(),
                    ReportCommand.CMD_ERROR_POLICY, (errors.isEmpty() ? FAIL : IGNORE).name()} );

            Annotation annotation1 = new Annotation( "comment1", 1, "author1" );
            new QueryRetrier()
                    .execute( client, new AttachTestRunAnnotation( testRunReport1.testRun().id(), annotation1 ), 1 );

            int addedAnnotations = 0;
            for ( BenchmarkGroupBenchmark bgb : testRunReport1.benchmarkGroupBenchmarks() )
            {
                addedAnnotations++;
                new QueryRetrier().execute( client,
                                            new AttachMetricsAnnotation( testRunReport1.testRun().id(),
                                                                         bgb.benchmark().name(),
                                                                         bgb.benchmarkGroup().name(),
                                                                         new Annotation( "comment", 1, "author" ) ),
                                            1 );
            }

            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "CompilationMetrics", 2, client );
            assertLabelCount( "PlanTree", 1, client );
            assertLabelCount( "Operator", 7, client );
            // Annotation specific
            assertLabelCount( "Annotation", 1 + addedAnnotations, client );

            new QueryRetrier().execute( client, new DeleteAnnotation( annotation1 ), 1 );

            // general
            new QueryRetrier().execute( client, new VerifyStoreSchema(), 1 );
            assertLabelCount( "TestRun", 1, client );
            assertLabelCount( "BenchmarkGroup", 1, client );
            assertLabelCount( "Benchmark", 2, client );
            assertLabelCount( "Metrics", 2, client );
            assertLabelCount( "Profiles", 2, client );
            assertLabelCount( "BenchmarkParams", 2, client );
            assertLabelCount( "Project", 1, client );
            assertLabelCount( "Neo4jConfig", 3, client );
            assertLabelCount( "Java", 1, client );
            assertLabelCount( "Environment", 1, client );
            assertLabelCount( "BenchmarkTool", 1, client );
            // plan specific
            assertLabelCount( "Plan", 2, client );
            assertLabelCount( "CompilationMetrics", 2, client );
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
        assertLabelCount( "CompilationMetrics", 0, client );
        assertLabelCount( "PlanTree", 0, client );
        assertLabelCount( "Operator", 0, client );
        // Annotation specific
        assertLabelCount( "Annotation", 0, client );
    }

    private static File createProfileFiles(
            TemporaryFolder temporaryFolder,
            BenchmarkGroup group,
            Benchmark benchmark1,
            Benchmark benchmark2 ) throws IOException
    {
        File topFolder = temporaryFolder.newFolder( "profiles" );
        temporaryFolder.newFile( "archive.tar.gz" );

        temporaryFolder.newFile( "profiles/" + group.name() + "." + benchmark1.name() + ".jfr" );
        temporaryFolder.newFile( "profiles/" + group.name() + "." + benchmark1.name() + "-jfr.svg" );
        temporaryFolder.newFile( "profiles/" + group.name() + "." + benchmark1.name() + ".async" );
        temporaryFolder.newFile( "profiles/" + group.name() + "." + benchmark1.name() + "-async.svg" );

        temporaryFolder.newFile( "profiles/" + group.name() + "." + benchmark2.name() + ".jfr" );
        temporaryFolder.newFile( "profiles/" + group.name() + "." + benchmark2.name() + "-jfr.svg" );
        return topFolder;
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
        Metrics metrics = new Metrics( MILLISECONDS, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 );
        Neo4jConfig neo4jConfig = Neo4jConfig.empty();
        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        Stream.of( benchmarks ).forEach( benchmark -> benchmarkGroupBenchmarkMetrics.add(
                benchmarkGroup,
                benchmark,
                metrics,
                neo4jConfig ) );
        return new TestRunReport(
                testRun,
                new BenchmarkConfig( emptyMap() ),
                Sets.newHashSet( new Project( Repository.NEO4J, "commit", "3.2.1", COMMUNITY, "branch", "owner" ) ),
                neo4jConfig,
                Environment.current(),
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
        Metrics metrics = new Metrics( MILLISECONDS, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 );
        Neo4jConfig neo4jConfig = Neo4jConfig.empty();
        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        Stream.of( benchmarks ).forEach( benchmark -> benchmarkGroupBenchmarkMetrics.add(
                benchmarkGroup,
                benchmark,
                metrics,
                neo4jConfig ) );
        return new TestRunReport(
                testRun,
                new BenchmarkConfig( emptyMap() ),
                Sets.newHashSet( new Project( Repository.NEO4J, "commit", "3.2.1", COMMUNITY, "branch", "owner" ),
                                 new Project( Repository.CAPS, "commit", "3.2.1", COMMUNITY, "branch", "owner" ) ),
                neo4jConfig,
                Environment.current(),
                benchmarkGroupBenchmarkMetrics,
                new BenchmarkTool( Repository.LDBC_BENCH, "commit", "neo-technology", "3.2" ),
                Java.current( "args" ),
                benchmarkPlans,
                errors );
    }

    public static Plan plan( String description )
    {
        PlanOperator leftLeaf1 = new PlanOperator( "left-leaf", 1, 2.0, 3 );
        leftLeaf1.addArgument( "a", "b" );
        PlanOperator leftLeaf2 = new PlanOperator( "left-leaf", 1, 2.0, 3 );
        leftLeaf1.addArgument( "a", "b" );

        PlanOperator rightLeaf1 = new PlanOperator( "right-leaf-1", 2, 3.0, 4 );
        rightLeaf1.addArgument( "a", "7" );
        rightLeaf1.addArgument( "b", "42" );
        PlanOperator rightLeaf2 = new PlanOperator( "right-leaf-2", 3, 4.0, 5 );
        rightLeaf2.addArgument( "c", "pies" );

        PlanOperator left = new PlanOperator( "left", 1, 1.0, 1 );
        left.addChild( leftLeaf1 );
        left.addChild( leftLeaf2 );

        PlanOperator right = new PlanOperator( "right", 1, 1.0, 1 );
        right.addArgument( "cakes", "not as good as pies" );
        right.addChild( rightLeaf1 );
        right.addChild( rightLeaf2 );

        PlanOperator root = new PlanOperator( "root", 0, 0.0, 0 );
        root.addArgument( "knock_knock", "who is there?" );
        root.addChild( left );
        root.addChild( right );

        Map<String,Long> planCompilationMetricsMap = new HashMap<>();
        planCompilationMetricsMap.put( "total", 9L );

        return new Plan(
                "cost",
                "cost",
                "cost",
                "compiled",
                "compiled",
                "compiled",
                "3.2",
                new PlanCompilationMetrics( planCompilationMetricsMap ),
                new PlanTree( description, root )
        );
    }
}
