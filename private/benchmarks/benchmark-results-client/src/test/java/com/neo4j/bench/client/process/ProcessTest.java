/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.process;

import com.google.common.collect.Lists;
import com.neo4j.bench.client.Main;
import com.neo4j.bench.client.util.Jvm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.client.util.TestDirectorySupport.createTempDirectoryPath;
import static com.neo4j.bench.client.util.TestDirectorySupport.createTempFile;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith( TestDirectoryExtension.class )
public class ProcessTest
{
    @Inject
    public TestDirectory temporaryFolder;

    public void shouldSelectRightJava() throws Exception
    {
        Jvm jvm = Jvm.defaultJvm();
        List<String> jvmArgs = Lists.newArrayList( "-Xmx4g" );
        ArrayList<String> toolCommandArgs = Lists.newArrayList( "help" );
        JvmProcessArgs jvmProcessArgs = JvmProcessArgs.argsForJvmProcess( Collections.emptyList(),
                                                                          jvm,
                                                                          jvmArgs,
                                                                          toolCommandArgs,
                                                                          Main.class );
        JvmProcess jvmProcess = JvmProcess.start( jvmProcessArgs );
        jvmProcess.waitFor();
    }

    @Test
    public void shouldLaunchSimpleProcessAndWriteItsOutputToFile() throws Exception
    {
        Path folder = createTempDirectoryPath( temporaryFolder.absolutePath() );
        Files.createFile( folder.resolve( "file1.txt" ) );
        Files.createFile( folder.resolve( "file2.txt" ) );

        File processOutput = createTempFile( temporaryFolder.absolutePath() );

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
