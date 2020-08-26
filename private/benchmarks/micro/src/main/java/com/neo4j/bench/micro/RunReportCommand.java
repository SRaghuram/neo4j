/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.tool.micro.RunReportParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.Runner;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Neo4j;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.reporter.ResultsReporter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.io.fs.FileUtils;

import static com.neo4j.bench.common.util.Args.concatArgs;
import static com.neo4j.bench.common.util.Args.splitArgs;
import static com.neo4j.bench.common.util.BenchmarkUtil.tryMkDir;

@Command( name = "run-export", description = "runs benchmarks and exports results as JSON" )
public class RunReportCommand extends BaseRunReportCommand
{
    @Option(
            type = OptionType.COMMAND,
            name = {"--aws-endpoint-url"},
            description = "AWS endpoint URL, used during testing",
            title = "AWS endpoint URL" )
    private String awsEndpointURL;

    @Option(
            type = OptionType.COMMAND,
            name = "--aws-region",
            description = "AWS region",
            title = "AWS region" )
    private String awsRegion = "eu-north-1";

    private static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_USER},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    private static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_PASSWORD},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    private static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    @Option( type = OptionType.COMMAND,
             name = {CMD_RESULTS_STORE_URI},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    public static final String CMD_S3_BUCKET = "--s3-bucket";
    @Option( type = OptionType.COMMAND,
             name = {CMD_S3_BUCKET},
             description = "S3 bucket profiles were uploaded to",
             title = "S3 bucket" )
    @Required
    private String s3Bucket;

    private static final int[] DEFAULT_THREAD_COUNTS = new int[]{1, Runtime.getRuntime().availableProcessors()};

    static final Neo4jConfig ADDITIONAL_CONFIG = Neo4jConfigBuilder.empty()
                                                                   .withSetting( BoltConnector.enabled, Boolean.FALSE.toString() )
                                                                   .build();

    @Override
    public void doRun( RunReportParams runReportParams )
    {
        TestRunReport testRunReport = runReport( runReportParams );
        ResultsReporter resultsReporter = new ResultsReporter( runReportParams.profilerOutput(),
                                                               testRunReport,
                                                               s3Bucket,
                                                               true,
                                                               resultsStoreUsername,
                                                               resultsStorePassword,
                                                               resultsStoreUri,
                                                               runReportParams.storesDir(),
                                                               awsEndpointURL );
        resultsReporter.report();

        try
        {
            System.out.println( "Deleting: " + runReportParams.storesDir().getAbsolutePath() );
            FileUtils.deleteDirectory( runReportParams.storesDir().toPath() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to to delete stores directory", e );
        }
    }

    private static TestRunReport runReport( RunReportParams runReportParams )
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.parse( runReportParams.parameterizedProfilers() );
        for ( ParameterizedProfiler profiler : profilers )
        {
            boolean errorOnMissingSecondaryEnvironmentVariables = true;
            profiler.profilerType().assertEnvironmentVariablesPresent( errorOnMissingSecondaryEnvironmentVariables );
        }

        // trim anything like '-M01' from end of Neo4j version string
        String neo4jVersion = Version.toSanitizeVersion( runReportParams.neo4jVersion() );

        Neo4jConfig baseNeo4jConfig = Neo4jConfigBuilder.withDefaults()
                                                        .mergeWith( Neo4jConfigBuilder.fromFile( runReportParams.neo4jConfigFile() ).build() )
                                                        .mergeWith( ADDITIONAL_CONFIG )
                                                        .build();

        String[] additionalJvmArgs = splitArgs( runReportParams.jvmArgsString(), " " );
        String[] jvmArgs = concatArgs( additionalJvmArgs, baseNeo4jConfig.getJvmArgs().toArray( new String[0] ) );

        // only used in interactive mode, to apply more (normally unsupported) benchmark annotations to JMH configuration
        boolean extendedAnnotationSupport = false;
        BenchmarksRunner runner = new BenchmarksRunner( baseNeo4jConfig,
                                                        JmhOptionsUtil.DEFAULT_FORK_COUNT,
                                                        JmhOptionsUtil.DEFAULT_ITERATION_COUNT,
                                                        JmhOptionsUtil.DEFAULT_ITERATION_DURATION,
                                                        extendedAnnotationSupport );
        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( BenchmarksRunner.class.getPackage().getName(),
                                                                              null == runReportParams.benchConfigFile()
                                                                              ? null
                                                                              : runReportParams.benchConfigFile().toPath() );
        ErrorReporter errorReporter = new ErrorReporter( runReportParams.errorPolicy() );

        if ( !runReportParams.storesDir().exists() )
        {
            System.out.println( "Creating stores directory: " + runReportParams.storesDir().getAbsolutePath() );
            tryMkDir( runReportParams.storesDir().toPath() );
        }

        Instant start = Instant.now();
        BenchmarkGroupBenchmarkMetrics resultMetrics = runner.run( suiteDescription,
                                                                   profilers,
                                                                   jvmArgs,
                                                                   DEFAULT_THREAD_COUNTS,
                                                                   runReportParams.storesDir().toPath(),
                                                                   errorReporter,
                                                                   splitArgs( runReportParams.jmhArgs(), " " ),
                                                                   Jvm.bestEffortOrFail( runReportParams.jvmFile() ),
                                                                   runReportParams.profilerOutput().toPath() );
        Instant finish = Instant.now();

        String testRunId = BenchmarkUtil.generateUniqueId();
        TestRun testRun = new TestRun(
                testRunId,
                Duration.between( start, finish ).toMillis(),
                start.toEpochMilli(),
                runReportParams.build(),
                runReportParams.parentBuild(),
                runReportParams.triggeredBy() );
        BenchmarkConfig benchmarkConfig = suiteDescription.toBenchmarkConfig();
        BenchmarkTool tool =
                new BenchmarkTool( Repository.MICRO_BENCH, runReportParams.toolCommit(), runReportParams.toolOwner(),
                                   runReportParams.toolBranch() );
        Java java = Java.current( String.join( " ", jvmArgs ) );

        TestRunReport testRunReport = new TestRunReport(
                testRun,
                benchmarkConfig,
                Sets.newHashSet( new Neo4j( runReportParams.neo4jCommit(),
                                            neo4jVersion,
                                            runReportParams.neo4jEdition(),
                                            runReportParams.neo4jBranch(),
                                            runReportParams.neo4jBranchOwner() ) ),
                baseNeo4jConfig,
                Environment.current(),
                resultMetrics,
                tool,
                java,
                Lists.newArrayList(),
                errorReporter.errors() );

        return testRunReport;
    }
}
