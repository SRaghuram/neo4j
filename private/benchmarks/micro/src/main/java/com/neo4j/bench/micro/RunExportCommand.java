/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.model.BenchmarkConfig;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.common.model.BenchmarkTool;
import com.neo4j.bench.common.model.Environment;
import com.neo4j.bench.common.model.Java;
import com.neo4j.bench.common.model.Neo4j;
import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.common.model.Repository;
import com.neo4j.bench.common.model.TestRun;
import com.neo4j.bench.common.model.TestRunReport;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.tool.micro.RunExportParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.Runner;
import com.neo4j.bench.jmh.api.config.JmhOptionsUtil;
import com.neo4j.bench.jmh.api.config.SuiteDescription;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.io.fs.FileUtils;

import static com.neo4j.bench.common.util.Args.concatArgs;
import static com.neo4j.bench.common.util.Args.splitArgs;
import static com.neo4j.bench.common.util.BenchmarkUtil.tryMkDir;

@Command( name = "run-export", description = "runs benchmarks and exports results as JSON" )
public class RunExportCommand extends BaseRunExportCommand
{
    private static final int[] DEFAULT_THREAD_COUNTS = new int[]{1, Runtime.getRuntime().availableProcessors()};

    static final Neo4jConfig ADDITIONAL_CONFIG = Neo4jConfigBuilder.empty()
                                                                   .withSetting( BoltConnector.enabled, Boolean.FALSE.toString() )
                                                                   .build();

    @Override
    public void doRun( RunExportParams runExportParams )
    {
        List<ParameterizedProfiler> profilers = ParameterizedProfiler.parse( runExportParams.parameterizedProfilers() );
        for ( ParameterizedProfiler profiler : profilers )
        {
            boolean errorOnMissingSecondaryEnvironmentVariables = true;
            profiler.profilerType().assertEnvironmentVariablesPresent( errorOnMissingSecondaryEnvironmentVariables );
        }

        // trim anything like '-M01' from end of Neo4j version string
        String neo4jVersion = Version.toSanitizeVersion( runExportParams.neo4jVersion() );

        Neo4jConfig baseNeo4jConfig = Neo4jConfigBuilder.withDefaults()
                                                        .mergeWith( Neo4jConfigBuilder.fromFile( runExportParams.neo4jConfigFile() ).build() )
                                                        .mergeWith( ADDITIONAL_CONFIG )
                                                        .build();

        String[] additionalJvmArgs = splitArgs( runExportParams.jvmArgsString(), " " );
        String[] jvmArgs = concatArgs( additionalJvmArgs, baseNeo4jConfig.getJvmArgs().toArray( new String[0] ) );

        // only used in interactive mode, to apply more (normally unsupported) benchmark annotations to JMH configuration
        boolean extendedAnnotationSupport = false;
        BenchmarksRunner runner = new BenchmarksRunner( baseNeo4jConfig,
                                                        JmhOptionsUtil.DEFAULT_FORK_COUNT,
                                                        JmhOptionsUtil.DEFAULT_ITERATION_COUNT,
                                                        JmhOptionsUtil.DEFAULT_ITERATION_DURATION,
                                                        extendedAnnotationSupport );
        SuiteDescription suiteDescription = Runner.createSuiteDescriptionFor( BenchmarksRunner.class.getPackage().getName(),
                                                                              null == runExportParams.benchConfigFile()
                                                                              ? null
                                                                              : runExportParams.benchConfigFile().toPath() );
        ErrorReporter errorReporter = new ErrorReporter( runExportParams.errorPolicy() );

        if ( !runExportParams.storesDir().exists() )
        {
            System.out.println( "Creating stores directory: " + runExportParams.storesDir().getAbsolutePath() );
            tryMkDir( runExportParams.storesDir().toPath() );
        }

        Instant start = Instant.now();
        BenchmarkGroupBenchmarkMetrics resultMetrics = runner.run( suiteDescription,
                                                                   profilers,
                                                                   jvmArgs,
                                                                   DEFAULT_THREAD_COUNTS,
                                                                   runExportParams.storesDir().toPath(),
                                                                   errorReporter,
                                                                   splitArgs( runExportParams.jmhArgs(), " " ),
                                                                   Jvm.bestEffortOrFail( runExportParams.jvmFile() ),
                                                                   runExportParams.profilerOutput().toPath() );
        Instant finish = Instant.now();

        try
        {
            System.out.println( "Deleting: " + runExportParams.storesDir().getAbsolutePath() );
            FileUtils.deleteRecursively( runExportParams.storesDir() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to to delete stores directory", e );
        }

        String testRunId = BenchmarkUtil.generateUniqueId();
        TestRun testRun = new TestRun(
                testRunId,
                Duration.between( start, finish ).toMillis(),
                start.toEpochMilli(),
                runExportParams.build(),
                runExportParams.parentBuild(),
                runExportParams.triggeredBy() );
        BenchmarkConfig benchmarkConfig = suiteDescription.toBenchmarkConfig();
        BenchmarkTool tool =
                new BenchmarkTool( Repository.MICRO_BENCH, runExportParams.toolCommit(), runExportParams.toolOwner(), runExportParams.toolBranch() );
        Java java = Java.current( String.join( " ", jvmArgs ) );

        TestRunReport testRunReport = new TestRunReport(
                testRun,
                benchmarkConfig,
                Sets.newHashSet( new Neo4j( runExportParams.neo4jCommit(),
                                            neo4jVersion,
                                            runExportParams.neo4jEdition(),
                                            runExportParams.neo4jBranch(),
                                            runExportParams.neo4jBranchOwner() ) ),
                baseNeo4jConfig,
                Environment.current(),
                resultMetrics,
                tool,
                java,
                Lists.newArrayList(),
                errorReporter.errors() );

        System.out.println( "Exporting results as JSON to: " + runExportParams.jsonPath().getAbsolutePath() );
        JsonUtil.serializeJson( runExportParams.jsonPath().toPath(), testRunReport );
    }
}
