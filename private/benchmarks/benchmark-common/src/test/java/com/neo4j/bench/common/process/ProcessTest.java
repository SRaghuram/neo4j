/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.process.JvmArgs;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@TestDirectoryExtension
public class ProcessTest
{
    private static final Logger LOG = LoggerFactory.getLogger( ProcessTest.class );

    @Inject
    public TestDirectory temporaryFolder;

    public static class JustForMain
    {

        public static void main( String[] args ) throws InterruptedException
        {
            Duration sleepMs = Duration.ofSeconds( 5 );
            Thread.sleep( sleepMs.toMillis() );
            LOG.debug( "Hello Work" );
        }
    }

    @Test
    void shouldSelectRightJava()
    {
        Jvm jvm = Jvm.defaultJvm();
        JvmArgs jvmArgs = JvmArgs.from( "-Xmx4g" );
        ArrayList<String> toolCommandArgs = Lists.newArrayList( "help" );
        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( Collections.emptyList(),
                                                                          jvm,
                                                                          jvmArgs,
                                                                          toolCommandArgs,
                                                                          JustForMain.class );
        JvmProcess jvmProcess = JvmProcess.start( jvmProcessArgs );
        jvmProcess.waitFor();
    }

    @Test
    void shouldLaunchSimpleProcessAndWriteItsOutputToFile() throws Exception
    {
        Path folder = temporaryFolder.directory( "folder" ).toPath();
        Files.createFile( folder.resolve( "file1.txt" ) );
        Files.createFile( folder.resolve( "file2.txt" ) );

        File processOutput = temporaryFolder.file( "processOutput" );
        Files.createFile( processOutput.toPath() );

        assertThat( "Expected process output to be empty", Files.lines( processOutput.toPath() ).count(), equalTo( 0L ) );

        ProcessWrapper process = ProcessWrapper.start( new ProcessBuilder()
                                                               .command( "ls", folder.toAbsolutePath().toString() )
                                                               .redirectOutput( processOutput ) );
        process.waitFor();

        String expectedFileLines = "file1.txt\n" +
                                   "file2.txt";
        String actualFileLines = Files.lines( processOutput.toPath() ).collect( Collectors.joining( "\n" ) );
        assertThat( expectedFileLines, equalTo( actualFileLines ) );
    }
}
