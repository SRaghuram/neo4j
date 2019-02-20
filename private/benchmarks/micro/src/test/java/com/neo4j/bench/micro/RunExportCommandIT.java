/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.micro.benchmarks.core.ReadById;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.Project;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.profiling.RecordingType;
import com.neo4j.bench.client.util.ErrorReporter;
import com.neo4j.bench.client.util.JsonUtil;
import com.neo4j.bench.client.util.Jvm;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.neo4j.bench.client.model.Edition.ENTERPRISE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RunExportCommandIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test( expected = RuntimeException.class )
    public void shouldThrowExceptionWhenNoBenchmarkIsEnabled() throws Exception
    {
        // Create empty Neo4j configuration file
        File neo4jConfigFile = temporaryFolder.newFile();
        Files.write( neo4jConfigFile.toPath(), Arrays.asList( "# empty config file" ) );

        // Create empty benchmark configuration file
        File benchmarkConfig = temporaryFolder.newFile();
        Files.write( neo4jConfigFile.toPath(), Arrays.asList( "# empty config file" ) );

        Path neo4jArchive = temporaryFolder.newFile().toPath();
        try ( InputStream inputStream = getClass().getResource( "/neo4j-enterprise-3.1.0-M09-unix.tar.gz" )
                                                  .openStream();
              OutputStream outputStream = Files.newOutputStream( neo4jArchive ) )
        {
            IOUtils.copy( inputStream, outputStream );
        }

        Path jsonFile = temporaryFolder.newFile().toPath();
        Path profileOutputDirectory = temporaryFolder.newFolder().toPath();
        Path storesDir = Paths.get( "benchmark_stores" );

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
                neo4jArchive,
                "-i 1 -wi 1 -r 1 -w 1 -f 1",
                profileOutputDirectory,
                storesDir,
                ErrorReporter.ErrorPolicy.FAIL,
                Jvm.defaultJvm(),
                "Trinity",
                Lists.newArrayList( ProfilerType.JFR ) );
        Main.main( commandArgs.toArray( new String[0] ) );
    }

    @Test
    public void shouldRunWithMinimalConfigurationWithSingleBenchmarkFromConfigFile() throws Exception
    {
        // Create empty Neo4j configuration file
        File neo4jConfigFile = temporaryFolder.newFile();
        Files.write( neo4jConfigFile.toPath(), Arrays.asList( "# empty config file" ) );

        // Create benchmark configuration file with only one benchmark enabled
        File benchmarkConfig = temporaryFolder.newFile();
        Files.write( neo4jConfigFile.toPath(), Arrays.asList( "# empty config file" ) );

        Class benchmark = ReadById.class;
        Main.main( new String[]{
                "config", "benchmarks",
                "--path", benchmarkConfig.getAbsolutePath(),
                benchmark.getName()
        } );

        Path neo4jArchive = temporaryFolder.newFile().toPath();
        try ( InputStream inputStream = getClass().getResource( "/neo4j-enterprise-3.1.0-M09-unix.tar.gz" )
                                                  .openStream();
              OutputStream outputStream = Files.newOutputStream( neo4jArchive ) )
        {
            IOUtils.copy( inputStream, outputStream );
        }

        Path jsonFile = temporaryFolder.newFile().toPath();
        Path profilerRecordingDirectory = temporaryFolder.newFolder().toPath();
        Path storesDir = Paths.get( "benchmark_stores" );

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
                neo4jArchive,
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
        assertThat( report.baseNeo4jConfig().toMap().size(), equalTo( 0 ) );
        assertThat( report.java().jvmArgs(), equalTo(
                "-Xms2g -Xmx2g -XX:+UseG1GC -XX:-OmitStackTraceInFastThrow -XX:+AlwaysPreTouch " +
                "-XX:+UnlockExperimentalVMOptions " +
                "-XX:+TrustFinalNonStaticFields -XX:+DisableExplicitGC -Djdk.tls.ephemeralDHKeySize=2048 " +
                "-Dunsupported.dbms.udc.source=tarball" ) );
        assertThat( report.testRun().build(), equalTo( 1L ) );
        HashMap<String,String> expectedBenchmarkConfig = new HashMap<>();
        expectedBenchmarkConfig.put( "com.neo4j.bench.micro.benchmarks.core.ReadById.format", "standard" );
        expectedBenchmarkConfig.put( "com.neo4j.bench.micro.benchmarks.core.ReadById.txMemory", "on_heap" );
        expectedBenchmarkConfig.put( "com.neo4j.bench.micro.benchmarks.core.ReadById", "true" );
        assertThat( report.benchmarkConfig().toMap(), equalTo( expectedBenchmarkConfig ) );
        int jfrCount = ProfilerTestUtil.recordingCountIn( profilerRecordingDirectory, RecordingType.JFR );
        assertThat( jfrCount,
                    equalTo( report.benchmarkGroupBenchmarkMetrics().toList().size() ) );
        int jfrFlameGraphCount = ProfilerTestUtil.recordingCountIn( profilerRecordingDirectory, RecordingType.JFR_FLAMEGRAPH );
        assertThat( jfrFlameGraphCount,
                    equalTo( report.benchmarkGroupBenchmarkMetrics().toList().size() ) );
    }
}
