/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.util.JsonUtil;
import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WorkspaceTest
{

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void openExistingWorkspace() throws Exception
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // macro workspace structure
        Files.createFile( workspaceBaseDir.resolve( "benchmark-infra-scheduler.jar" ) );
        Files.createFile( workspaceBaseDir.resolve( "neo4j-enterprise-3.3.10-unix.tar.gz" ) );
        Files.createDirectories( workspaceBaseDir.resolve( "macro/target" ) );
        Files.createFile( workspaceBaseDir.resolve( "macro/target/macro.jar" ) );
        Files.createFile( workspaceBaseDir.resolve( "macro/run-report-benchmark.sh" ) );
        // when
        Workspace workspace = Workspace.create( workspaceBaseDir )
                                       .withArtifacts(
                                               "benchmark-infra-scheduler.jar",
                                               "neo4j-enterprise-3.3.10-unix.tar.gz",
                                               "macro/target/macro.jar",
                                               "macro/run-report-benchmark.sh"
                                       ).build();
        // then
        assertNotNull( workspace );
        // when
        Path path = workspace.get( "benchmark-infra-scheduler.jar" );
        assertTrue( Files.isRegularFile( path ) );
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
                     .withArtifacts(
                             "benchmark-infra-scheduler.jar",
                             "neo4j-enterprise-3.3.10-unix.tar.gz",
                             "macro/target/macro.jar",
                             "macro/run-report-benchmark.sh"
                     ).build();
        } );
    }

    @Test
    public void filterFilesRecursively() throws Exception
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // create files & directories struture
        Files.createDirectories( workspaceBaseDir.resolve( "a" ) );
        Files.createFile( workspaceBaseDir.resolve( "a" ).resolve( "a.txt" ) );
        Files.createDirectories( workspaceBaseDir.resolve( "a" ).resolve( "b" ) );
        Files.createFile( workspaceBaseDir.resolve( "a" ).resolve( "b" ).resolve( "b.txt" ) );
        // when
        Workspace workspace = Workspace.create( workspaceBaseDir ).withFilesRecursively( TrueFileFilter.INSTANCE ).build();
        // then
        assertThat( workspace.allArtifacts(), containsInAnyOrder(
                workspaceBaseDir.resolve( "a/a.txt" ),
                workspaceBaseDir.resolve( "a/b/b.txt" ) ) );
        // when
        workspace = Workspace.create( workspaceBaseDir ).withFilesRecursively( new NameFileFilter( "b.txt" ) ).build();
        // then
        assertThat( workspace.allArtifacts(), contains( workspaceBaseDir.resolve( "a/b/b.txt" ) ) );
    }

    @Test( expected = RuntimeException.class )
    public void throwErrorOnNonExistingWorkspacePath() throws Exception
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // macro workspace structure
        Files.createFile( workspaceBaseDir.resolve( "benchmark-infra-scheduler.jar" ) );
        Workspace workspace = Workspace.create( workspaceBaseDir )
                                       .withArtifacts(
                                                "benchmark-infra-scheduler.jar"
                                       ).build();
        // when
        Path path = workspace.get( "artifact.jar" );
    }

    @Test
    public void shouldSerializeAndDeserializerWorkspace() throws IOException
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // macro workspace structure
        Files.createFile( workspaceBaseDir.resolve( "neo4j-enterprise-3.3.10-unix.tar.gz" ) );
        Files.createDirectories( workspaceBaseDir.resolve( "macro" ) );
        Files.createFile( workspaceBaseDir.resolve( "macro/run-report-benchmark.sh" ) );
        Workspace workspace = Workspace.create( workspaceBaseDir )
                                       .withArtifacts(
                                               "neo4j-enterprise-3.3.10-unix.tar.gz",
                                               "macro/run-report-benchmark.sh"
                                                     ).build();
        // when
        String json = JsonUtil.serializeJson( workspace );
        // then
        assertThat( JsonUtil.deserializeJson( json, Workspace.class ).allArtifacts(), equalTo( workspace.allArtifacts() ) );
    }
}
