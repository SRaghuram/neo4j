/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigurationException;
import com.neo4j.bench.micro.benchmarks.core.ReadById;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Project;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.model.options.Edition.ENTERPRISE;
import static com.neo4j.bench.model.util.MapPrinter.prettyPrint;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
class RunExportCommandIT
{
    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void shouldThrowExceptionWhenNoBenchmarkIsEnabled()
    {
        var exception = assertThrows( BenchmarkConfigurationException.class, () ->
        {
            // Create empty Neo4j configuration file
            File neo4jConfigFile = temporaryFolder.file( "neo4j.conf" );
            try ( InputStream inputStream = getClass().getResource( "/neo4j.conf" )
                                                      .openStream();
                  OutputStream outputStream = Files.newOutputStream( neo4jConfigFile.toPath() ) )
            {
                IOUtils.copy( inputStream, outputStream );
            }

            // Create empty benchmark configuration file
            File benchmarkConfig = temporaryFolder.file( "benchmark.config" );
            Files.write( benchmarkConfig.toPath(), List.of( "# empty config file" ) );

            Path jsonFile = temporaryFolder.file( "file.json" ).toPath();
            Path profileOutputDirectory = temporaryFolder.directory( "output" ).toPath();
            Path storesDir = temporaryFolder.directory( "benchmark_stores" ).toPath();

            List<String> commandArgs = RunExportCommand.argsFor(
                    jsonFile,
                    "abc123",
                    "2.2.10",
                    ENTERPRISE,
                    "master",
                    "Trinity",
                    neo4jConfigFile.toPath(),
                    "2",
                    "Trinity",
                    "master",
                    1,
                    1,
                    "-Xms2g -Xmx2g",
                    benchmarkConfig.toPath(),
                    "-i 1 -wi 1 -r 1 -w 1 -f 1",
                    profileOutputDirectory,
                    storesDir,
                    ErrorReporter.ErrorPolicy.FAIL,
                    Jvm.defaultJvm(),
                    "Trinity",
                    Lists.newArrayList( ProfilerType.JFR ) );
            Main.main( commandArgs.toArray( new String[0] ) );
        } );
        assertEquals( "Validation Failed\n" +
                      "\tNo benchmarks were configured!", exception.getMessage() );
    }

    @Test
    void shouldRunWithMinimalConfigurationWithSingleBenchmarkFromConfigFile() throws Exception
    {
        // Create empty Neo4j configuration file
        File neo4jConfigFile = temporaryFolder.file( "neo4j.conf" );
        try ( InputStream inputStream = getClass().getResource( "/neo4j.conf" )
                                                  .openStream();
              OutputStream outputStream = Files.newOutputStream( neo4jConfigFile.toPath() ) )
        {
            IOUtils.copy( inputStream, outputStream );
        }

        // Create benchmark configuration file with only one benchmark enabled
        File benchmarkConfig = temporaryFolder.file( "benchmark.config" );

        Class<?> benchmark = ReadById.class;
        Main.main( new String[]{
                "config", "benchmarks",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmark.getName()
        } );

        Path jsonFile = temporaryFolder.file( "file.json" ).toPath();
        Path profilerRecordingDirectory = temporaryFolder.directory( "output" ).toPath();
        Path storesDir = temporaryFolder.directory( "benchmark_stores" ).toPath();

        List<String> commandArgs = RunExportCommand.argsFor(
                jsonFile,
                "abc123",
                "2.2.10",
                ENTERPRISE,
                "master",
                "Trinity",
                neo4jConfigFile.toPath(),
                "2",
                "Trinity",
                "master",
                1,
                1,
                "-Xms2g -Xmx2g",
                benchmarkConfig.toPath(),
                "-i 1 -wi 1 -r 1 -w 1 -f 1",
                profilerRecordingDirectory,
                storesDir,
                ErrorReporter.ErrorPolicy.FAIL,
                Jvm.defaultJvm(),
                "Trinity",
                Lists.newArrayList( ProfilerType.JFR ) );
        Main.main( commandArgs.toArray( new String[0] ) );

        TestRunReport report = JsonUtil.deserializeJson( jsonFile, TestRunReport.class );
        assertThat( report.projects(),
                    equalTo( Sets.newHashSet( new Project( Repository.NEO4J, "abc123", "2.2.10", ENTERPRISE, "master", "Trinity" ) ) ) );
        BenchmarkTool expectedBenchmarkTool =
                new BenchmarkTool( Repository.MICRO_BENCH, "2", "Trinity", "master" );
        assertThat( report.benchmarkTool(), equalTo( expectedBenchmarkTool ) );

        int expectedConfigSize = Neo4jConfigBuilder.withDefaults()
                                                   .mergeWith( Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build() )
                                                   .mergeWith( RunExportCommand.ADDITIONAL_CONFIG )
                                                   .build()
                                                   .toMap()
                                                   .size();

        assertThat( prettyPrint( report.baseNeo4jConfig().toMap() ), report.baseNeo4jConfig().toMap().size(), equalTo( expectedConfigSize ) );
        assertThat( report.java().jvmArgs(), equalTo(
                "-Xms2g -Xmx2g -XX:+UseG1GC -XX:-OmitStackTraceInFastThrow -XX:+AlwaysPreTouch " +
                "-XX:+UnlockExperimentalVMOptions " +
                "-XX:+TrustFinalNonStaticFields -XX:+DisableExplicitGC -Djdk.tls.ephemeralDHKeySize=2048 " +
                "-Djdk.tls.rejectClientInitiatedRenegotiation=true -Dunsupported.dbms.udc.source=tarball" ) );
        assertThat( report.testRun().build(), equalTo( 1L ) );
        HashMap<String,String> expectedBenchmarkConfig = new HashMap<>();
        expectedBenchmarkConfig.put( "com.neo4j.bench.micro.benchmarks.core.ReadById.format", "standard" );
        expectedBenchmarkConfig.put( "com.neo4j.bench.micro.benchmarks.core.ReadById.txMemory", "default" );
        expectedBenchmarkConfig.put( "com.neo4j.bench.micro.benchmarks.core.ReadById", "true" );
        assertThat( report.benchmarkConfig().toMap(), equalTo( expectedBenchmarkConfig ) );
        int jfrCount = ProfilerTestUtil.recordingCountIn( profilerRecordingDirectory, RecordingType.JFR );
        assertThat( jfrCount,
                    equalTo( report.benchmarkGroupBenchmarkMetrics().toList().size() ) );
        // in 4.0 we do NOT generate Flamegraphs
        int jfrFlameGraphCount = ProfilerTestUtil.recordingCountIn( profilerRecordingDirectory, RecordingType.JFR_FLAMEGRAPH );
        assertThat( jfrFlameGraphCount,
                    equalTo( 0 ) );
    }
}
