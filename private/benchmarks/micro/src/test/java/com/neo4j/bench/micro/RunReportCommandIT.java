/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.ErrorReporter;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigurationException;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.model.options.Edition.ENTERPRISE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
class RunReportCommandIT
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

            List<String> commandArgs = RunReportCommand.argsFor(
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
                    Lists.newArrayList( ProfilerType.GC ),
                    "neo4j://localhost", "user", "pass",
                    "s3://benchmarking.neo.hq"
            );
        } );
        assertEquals( "Validation Failed\n" +
                      "\tNo benchmarks were configured!", exception.getMessage() );
    }
}
