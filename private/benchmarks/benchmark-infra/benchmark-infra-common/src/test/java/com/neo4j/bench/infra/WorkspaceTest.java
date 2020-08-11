/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WorkspaceTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldFindAllFilesRecursively() throws IOException
    {
        // given
        Path folder = temporaryFolder.newFolder().toPath();
        Path childFolder1 = folder.resolve( "folder1" );
        Path childFolder2 = childFolder1.resolve( "folder2" );
        Files.createDirectories( childFolder2 );

        Path childFile1 = Files.createFile( childFolder1.resolve( "file1.txt" ) );
        Path childFile2 = Files.createFile( childFolder2.resolve( "file2.txt" ) );

        BenchmarkUtil.assertFileExists( childFile1 );
        BenchmarkUtil.assertFileExists( childFile2 );

        // when
        Workspace workspace = Workspace.create( folder ).withFilesRecursively( TrueFileFilter.INSTANCE ).build();

        // then
        List<Path> workspaceFiles = workspace.allArtifacts();
        assertThat( format( "Found: %s", workspaceFiles ), workspaceFiles.size(), equalTo( 2 ) );
        assertThat( format( "Found: %s", workspaceFiles ), workspaceFiles, containsInAnyOrder( childFile1, childFile2 ) );
        List<String> workspaceKeys = workspace.allArtifactKeys();
        assertThat( format( "Found: %s", workspaceKeys ), workspaceKeys, containsInAnyOrder( "folder1/file1.txt", "folder1/folder2/file2.txt" ) );
    }

    @Test
    public void shouldFindSpecificFile() throws IOException
    {
        // given
        Path folder = temporaryFolder.newFolder().toPath();
        Path childFolder1 = folder.resolve( "folder1" );
        Path childFolder2 = childFolder1.resolve( "folder2" );
        Files.createDirectories( childFolder2 );

        Path childFile1 = Files.createFile( childFolder1.resolve( "file1.txt" ) );
        Path childFile2 = Files.createFile( childFolder2.resolve( "file2.txt" ) );

        BenchmarkUtil.assertFileExists( childFile1 );
        BenchmarkUtil.assertFileExists( childFile2 );

        // when
        Workspace workspace = Workspace.create( folder ).withFilesRecursively( new IOFileFilter()
        {

            @Override
            public boolean accept( File file )
            {
                return file.equals( childFile2.toFile() );
            }

            @Override
            public boolean accept( File dir, String name )
            {
                return dir.equals( childFile2.getParent().toFile() ) && name.equals( childFile2.getFileName().toString() );
            }
        } ).build();

        // then
        List<Path> workspaceFiles = workspace.allArtifacts();
        assertThat( format( "Found: %s", workspaceFiles ), workspaceFiles.size(), equalTo( 1 ) );
        assertThat( format( "Found: %s", workspaceFiles ), workspaceFiles, containsInAnyOrder( childFile2 ) );
        List<String> workspaceKeys = workspace.allArtifactKeys();
        assertThat( format( "Found: %s", workspaceKeys ), workspaceKeys, containsInAnyOrder( "folder1/folder2/file2.txt" ) );
    }

    @Test
    public void openExistingWorkspace() throws Exception
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // macro workspace structure
        Path schedulerJar = Files.createFile( workspaceBaseDir.resolve( "benchmark-infra-scheduler.jar" ) );
        Path neo4j = Files.createFile( workspaceBaseDir.resolve( "neo4j-enterprise-3.3.10-unix.tar.gz" ) );
        Files.createDirectories( workspaceBaseDir.resolve( "macro/target" ) );
        Path macroJar = Files.createFile( workspaceBaseDir.resolve( "macro/target/macro.jar" ) );
        Path script = Files.createFile( workspaceBaseDir.resolve( "macro/run-report-benchmark.sh" ) );
        // when
        Workspace workspace = Workspace.create( workspaceBaseDir )
                                       .withArtifact( Workspace.WORKER_JAR, "benchmark-infra-scheduler.jar" )
                                       .withArtifact( Workspace.NEO4J_ARCHIVE, "neo4j-enterprise-3.3.10-unix.tar.gz" )
                                       .withArtifact( Workspace.BENCHMARKING_JAR, "macro/target/macro.jar" )
                                       .withArtifact( Workspace.RUN_SCRIPT, "macro/run-report-benchmark.sh" )
                                       .build();
        // then
        assertNotNull( workspace );
        // when
        assertTrue( Files.isRegularFile( workspace.get( Workspace.WORKER_JAR ) ) );
        assertTrue( Files.isRegularFile( workspace.get( Workspace.NEO4J_ARCHIVE ) ) );
        assertTrue( Files.isRegularFile( workspace.get( Workspace.BENCHMARKING_JAR ) ) );
        assertTrue( Files.isRegularFile( workspace.get( Workspace.RUN_SCRIPT ) ) );
        List<Path> artifacts = workspace.allArtifacts();
        assertThat( artifacts, containsInAnyOrder( schedulerJar, neo4j, macroJar, script ) );
    }

    @Test
    public void throwExceptionIfWorkspaceIsEmpty() throws Exception
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // when, workspace is empty
        assertThrows( IllegalStateException.class, () ->
        {
            Workspace.create( workspaceBaseDir )
                     .withArtifact( Workspace.WORKER_JAR, "benchmark-infra-scheduler.jar" )
                     .withArtifact( Workspace.NEO4J_ARCHIVE, "neo4j-enterprise-3.3.10-unix.tar.gz" )
                     .withArtifact( Workspace.BENCHMARKING_JAR, "macro/target/macro.jar" )
                     .withArtifact( Workspace.RUN_SCRIPT, "macro/run-report-benchmark.sh" )
                     .build();
        } );
    }

    @Test( expected = RuntimeException.class )
    public void throwErrorOnRetrievalOfNonExistingArtifact() throws Exception
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // macro workspace structure
        Files.createFile( workspaceBaseDir.resolve( "benchmark-infra-scheduler.jar" ) );
        Workspace workspace = Workspace.create( workspaceBaseDir )
                                       .withArtifact( Workspace.WORKER_JAR, "benchmark-infra-scheduler.jar" )
                                       .build();
        // when
        Path nonExistingArtifact = workspace.get( "does_not_exist.jar" );
    }

    @Test
    public void shouldSerializeAndDeserializerWorkspace() throws IOException
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // macro workspace structure
        Path neo4j = Files.createFile( workspaceBaseDir.resolve( "neo4j-enterprise-3.3.10-unix.tar.gz" ) );
        Files.createDirectories( workspaceBaseDir.resolve( "macro" ) );
        Path script = Files.createFile( workspaceBaseDir.resolve( "macro/run-report-benchmark.sh" ) );
        Workspace workspace = Workspace.create( workspaceBaseDir )
                                       .withArtifact( Workspace.NEO4J_ARCHIVE, "neo4j-enterprise-3.3.10-unix.tar.gz" )
                                       .withArtifact( Workspace.RUN_SCRIPT, "macro/run-report-benchmark.sh" )
                                       .build();
        // when
        String json = JsonUtil.serializeJson( workspace );
        // then
        Workspace deserializedWorkspace = JsonUtil.deserializeJson( json, Workspace.class );
        List<Path> deserializedArtifacts = deserializedWorkspace.allArtifacts();
        assertThat( deserializedArtifacts, equalTo( workspace.allArtifacts() ) );
        assertThat( format( "Found: %s", deserializedArtifacts ), deserializedArtifacts, containsInAnyOrder( neo4j, script ) );
        List<String> workspaceKeys = deserializedWorkspace.allArtifactKeys();
        assertThat( format( "Found: %s", workspaceKeys ), workspaceKeys, containsInAnyOrder( Workspace.NEO4J_ARCHIVE, Workspace.RUN_SCRIPT ) );
    }

    @Test
    public void shouldFindAllFilesRecursivelyAndRemoveProfiles() throws IOException
    {
        // given
        Path folder = temporaryFolder.newFolder().toPath();
        Path childFolder1 = folder.resolve( "folder1" );
        Path childFolder2 = childFolder1.resolve( "folder2" );
        Files.createDirectories( childFolder2 );

        Path childFile1 = Files.createFile( childFolder1.resolve( "file1.txt" ) );
        Path childFile2 = Files.createFile( childFolder2.resolve( "file2.txt" ) );
        Path childFile3 = Files.createFile( childFolder1.resolve( "file3" + RecordingType.ASYNC.extension() ) );
        Path childFile4 = Files.createFile( childFolder2.resolve( "file4" + RecordingType.JFR.extension() ) );

        BenchmarkUtil.assertFileExists( childFile1 );
        BenchmarkUtil.assertFileExists( childFile2 );
        BenchmarkUtil.assertFileExists( childFile3 );
        BenchmarkUtil.assertFileExists( childFile4 );

        // when
        Workspace workspace = Workspace.create( folder ).withFilesRecursively( new IgnoreProfilerFileFilter() ).build();

        // then
        List<Path> workspaceFiles = workspace.allArtifacts();
        assertThat( format( "Found: %s", workspaceFiles ), workspaceFiles.size(), equalTo( 2 ) );
        assertThat( format( "Found: %s", workspaceFiles ), workspaceFiles, containsInAnyOrder( childFile1, childFile2 ) );
        List<String> workspaceKeys = workspace.allArtifactKeys();
        assertThat( format( "Found: %s", workspaceKeys ), workspaceKeys, containsInAnyOrder( "folder1/file1.txt", "folder1/folder2/file2.txt" ) );
    }
}
