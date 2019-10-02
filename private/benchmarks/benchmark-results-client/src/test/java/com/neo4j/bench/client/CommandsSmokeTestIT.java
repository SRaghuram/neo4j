/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.SyntheticStoreGenerator.GenerationResult;
import com.neo4j.bench.client.SyntheticStoreGenerator.ToolBenchGroup;
import com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Repository;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.BenchmarkUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.METRICS;
import static com.neo4j.bench.client.queries.annotation.CreateAnnotations.AnnotationTarget.TEST_RUN;
import static com.neo4j.bench.common.model.Repository.LDBC_BENCH;
import static com.neo4j.bench.common.model.Repository.MACRO_BENCH;
import static com.neo4j.bench.common.model.Repository.MICRO_BENCH;
import static com.neo4j.bench.common.model.Repository.NEO4J;
import static com.neo4j.bench.common.options.Edition.ENTERPRISE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class CommandsSmokeTestIT
{
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier( false );
    private static final BenchmarkGroup MACRO_COMPAT_GROUP_1 = new BenchmarkGroup( "Group1" );
    private static final BenchmarkGroup MACRO_COMPAT_GROUP_2 = new BenchmarkGroup( "Group2" );
    private static final BenchmarkGroup LDBC_READ = new BenchmarkGroup( "LdbcSnbInteractive-Read" );
    private static final BenchmarkGroup LDBC_WRITE = new BenchmarkGroup( "LdbcSnbInteractive-Write" );

    private final Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( temporaryFolder ).around( neo4j );
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    private final ToolBenchGroup[] toolBenchGroups = {ToolBenchGroup.from( MICRO_BENCH, "Cypher", 5 ),
                                                      ToolBenchGroup.from( MICRO_BENCH, "Values", 5 ),
                                                      ToolBenchGroup.from( MACRO_BENCH, MACRO_COMPAT_GROUP_1, macroBench(), macroBench() ),
                                                      ToolBenchGroup.from( MACRO_BENCH, MACRO_COMPAT_GROUP_2, macroBench(), macroBench() ),
                                                      ToolBenchGroup.from( LDBC_BENCH, LDBC_WRITE, ldbcBench( "Core API", 10 ) ),
                                                      ToolBenchGroup.from( LDBC_BENCH, LDBC_READ, ldbcBench( "Cypher", 1 ) )};

    @Test
    public void shouldRunAnnotateTestRunsCommand() throws Exception
    {
        GenerationResult generationResult = createSyntheticResultsStore();

        List<Long> packagingBuildIds = generationResult.packagingBuildIds();
        // select lowest parent build ID, to maximize probability that every tool has at least one test run with higher parent build
        Long packagingBuildId = packagingBuildIds.get( 0 );
        List<Repository> benchmarkTools = Lists.newArrayList( MICRO_BENCH, MACRO_BENCH, LDBC_BENCH );

        long testRunAnnotationCountBefore = testRunAnnotationCount();
        long metricsAnnotationCountBefore = metricsAnnotationCount();

        runAnnotateCommand( packagingBuildId, benchmarkTools, "3.0", Sets.newHashSet( TEST_RUN ) );

        long testRunAnnotationCountAfter1 = testRunAnnotationCount();
        long metricsAnnotationCountAfter1 = metricsAnnotationCount();
        assertThat( "Should create exactly one annotation per benchmark tool - on the latest test run (after provided parent build ID) for that tool",
                    testRunAnnotationCountAfter1,
                    equalTo( testRunAnnotationCountBefore + toolBenchGroups.length ) );
        assertThat( "Should not have created any more :Metrics annotations at this point",
                    metricsAnnotationCountAfter1,
                    equalTo( metricsAnnotationCountBefore ) );

        runAnnotateCommand( packagingBuildId, benchmarkTools, "3.0", Sets.newHashSet( METRICS ) );

        long testRunAnnotationCountAfter2 = testRunAnnotationCount();
        long metricsAnnotationCountAfter2 = metricsAnnotationCount();
        assertThat( "Should not have created any more :TestRun annotations at this point",
                    testRunAnnotationCountAfter2,
                    equalTo( testRunAnnotationCountAfter1 ) );
        assertThat( "Should create exactly one annotation per benchmark - at the latest test run (after provided parent build ID) it appears in",
                    metricsAnnotationCountAfter2,
                    equalTo( metricsAnnotationCountAfter1 + generationResult.benchmarks() ) );

        runAnnotateCommand( packagingBuildId, benchmarkTools, "3.0", Sets.newHashSet( TEST_RUN, METRICS ) );

        long testRunAnnotationCountAfter3 = testRunAnnotationCount();
        long metricsAnnotationCountAfter3 = metricsAnnotationCount();
        assertThat( "Should create exactly one annotation per benchmark tool - on the latest test run (after provided parent build ID) for that tool",
                    testRunAnnotationCountAfter3,
                    equalTo( testRunAnnotationCountAfter2 + toolBenchGroups.length ) );
        assertThat( "Should create exactly one annotation per benchmark - at the latest test run (after provided parent build ID) it appears in",
                    metricsAnnotationCountAfter3,
                    equalTo( metricsAnnotationCountAfter2 + generationResult.benchmarks() ) );
    }

    private void runAnnotateCommand( Long packagingBuildId,
                                     List<Repository> benchmarkTools,
                                     String neo4jSeries,
                                     Set<AnnotationTarget> annotationTargets ) throws Exception
    {
        List<String> args = AnnotatePackagingBuildCommand.argsFor( USERNAME,
                                                                   PASSWORD,
                                                                   neo4j.boltURI(),
                                                                   packagingBuildId,
                                                                   "comment " + UUID.randomUUID(),
                                                                   "author " + UUID.randomUUID(),
                                                                   neo4jSeries,
                                                                   benchmarkTools,
                                                                   annotationTargets );
        Main.main( args.stream().toArray( String[]::new ) );
    }

    private long testRunAnnotationCount()
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            StatementResult result = client.session().run( "RETURN size((:TestRun)-[:WITH_ANNOTATION]->(:Annotation)) AS testRunAnnotations" );
            return result.next().get( "testRunAnnotations" ).asLong();
        }
    }

    private long metricsAnnotationCount()
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            StatementResult result = client.session().run( "RETURN size((:Metrics)-[:WITH_ANNOTATION]->(:Annotation)) AS metricsAnnotations" );
            return result.next().get( "metricsAnnotations" ).asLong();
        }
    }

    @Test
    public void shouldRunCompareVersionsCommand() throws Exception
    {
        GenerationResult generationResult = createSyntheticResultsStore();

        Path outputDir = temporaryFolder.newFolder().toPath();
        List<String> versionComparisonArgs = CompareVersionsCommand.argsFor( USERNAME,
                                                                             PASSWORD,
                                                                             neo4j.boltURI(),
                                                                             "3.0.0",
                                                                             "3.0.1",
                                                                             1.0,
                                                                             outputDir );
        Main.main( versionComparisonArgs.stream().toArray( String[]::new ) );

        assertThat( fileCount( outputDir ), equalTo( 4L ) );
        Path microComparisonCsv = outputDir.resolve( CompareVersionsCommand.MICRO_COMPARISON_FILENAME );
        Path microCoverageCsv = outputDir.resolve( CompareVersionsCommand.MICRO_COVERAGE_FILENAME );
        Path ldbcComparisonCsv = outputDir.resolve( CompareVersionsCommand.LDBC_COMPARISON_FILENAME );
        Path macroComparisonCsv = outputDir.resolve( CompareVersionsCommand.MACRO_COMPARISON_FILENAME );

        assertTrue( Files.exists( microComparisonCsv ) );
        assertTrue( Files.exists( microCoverageCsv ) );
        assertTrue( Files.exists( ldbcComparisonCsv ) );
        assertTrue( Files.exists( macroComparisonCsv ) );

        // Check for Micro comparison CSV
        assertThat( "Micro comparison file should contain correct number of entries",
                    lineCount( microComparisonCsv ),
                    equalTo( generationResult.benchmarksInTool( MICRO_BENCH.projectName() ) + 1L /*header*/ ) );

        // Check Micro coverage CSV
        assertThat( "Micro coverage file should contain correct number of entries",
                    lineCount( microCoverageCsv ),
                    equalTo( generationResult.benchmarksInTool( MICRO_BENCH.projectName() ) + 1L /*header*/ ) );

        // Check for LDBC comparison CSV
        assertThat( "LDBC comparison file should contain correct number of entries",
                    lineCount( ldbcComparisonCsv ),
                    equalTo( generationResult.benchmarksInToolAndGroups( LDBC_BENCH.projectName(),
                                                                         LDBC_WRITE.name(),
                                                                         LDBC_READ.name() ) + 1L /*header*/ ) );

        // Check for Macro comparison CSV
        assertThat( "Macro comparison file should contain correct number of entries",
                    lineCount( macroComparisonCsv ),
                    equalTo( generationResult.benchmarksInToolAndGroups( MACRO_BENCH.projectName(),
                                                                         MACRO_COMPAT_GROUP_1.name(),
                                                                         MACRO_COMPAT_GROUP_2.name() ) + 1L /*header*/ ) );
    }

    private static long fileCount( Path folder ) throws IOException
    {
        BenchmarkUtil.assertDirectoryExists( folder );
        try ( Stream<Path> files = Files.list( folder ) )
        {
            return files.count();
        }
    }

    private static long lineCount( Path file ) throws IOException
    {
        try ( Stream<String> lines = Files.lines( file ) )
        {
            return lines.count();
        }
    }

    private GenerationResult createSyntheticResultsStore()
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 5 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroups( toolBenchGroups )
                .withNeo4jVersions( "4.0.1", "3.0.1", "3.0.0" )
                .withNeo4jEditions( ENTERPRISE )
                .withSettingsInConfig( 1 )
                .withNeo4jBranchOwners( "neo4j" )
                .withToolBranchOwners( "neo-technology" )
                .withProjects( NEO4J )
                .withPrintout( false )
                .build();
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema(), 0 );
            return generator.generate( client );
        }
    }

    private Benchmark ldbcBench( String api, int scaleFactor )
    {
        Map<String,String> paramsMap = new HashMap<>();
        paramsMap.put( "api", api );
        paramsMap.put( "scale_factor", Integer.toString( scaleFactor ) );
        return Benchmark.benchmarkFor( "Description",
                                       "Summary",
                                       Benchmark.Mode.THROUGHPUT,
                                       paramsMap );
    }

    private Benchmark macroBench()
    {
        Map<String,String> paramsMap = new HashMap<>();
        paramsMap.put( "execution_mode", ExecutionMode.EXECUTE.name() );
        paramsMap.put( "runtime", Runtime.DEFAULT.name() );
        paramsMap.put( "planner", Planner.DEFAULT.name() );
        paramsMap.put( "deployment", Deployment.embedded().parsableValue() );
        return Benchmark.benchmarkFor( "Description",
                                       "Query " + UUID.randomUUID(),
                                       Benchmark.Mode.LATENCY,
                                       paramsMap );
    }
}
