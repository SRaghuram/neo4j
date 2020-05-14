/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.model.util.JsonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
                                       .withArtifact( Workspace.WORKER_JAR, "benchmark-infra-scheduler.jar" )
                                       .withArtifact( Workspace.NEO4J_ARCHIVE, "neo4j-enterprise-3.3.10-unix.tar.gz" )
                                       .withArtifact( Workspace.BENCHMARKING_JAR, "macro/target/macro.jar" )
                                       .withArtifact( Workspace.RUN_SCRIPT, "macro/run-report-benchmark.sh" )
                                       .build();
        // then
        assertNotNull( workspace );
        // when
        Path path = workspace.get( Workspace.WORKER_JAR );
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
                     .withArtifact( Workspace.WORKER_JAR, "benchmark-infra-scheduler.jar" )
                     .withArtifact( Workspace.NEO4J_ARCHIVE, "neo4j-enterprise-3.3.10-unix.tar.gz" )
                     .withArtifact( Workspace.BENCHMARKING_JAR, "macro/target/macro.jar" )
                     .withArtifact( Workspace.RUN_SCRIPT, "macro/run-report-benchmark.sh" )
                     .build();
        } );
    }

    @Test( expected = RuntimeException.class )
    public void throwErrorOnNonExistingWorkspacePath() throws Exception
    {
        // given
        Path workspaceBaseDir = temporaryFolder.newFolder().toPath();
        // macro workspace structure
        Files.createFile( workspaceBaseDir.resolve( "benchmark-infra-scheduler.jar" ) );
        Workspace workspace = Workspace.create( workspaceBaseDir )
                                       .withArtifact( Workspace.WORKER_JAR, "benchmark-infra-scheduler.jar"
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
                                       .withArtifact( Workspace.NEO4J_ARCHIVE, "neo4j-enterprise-3.3.10-unix.tar.gz" )
                                       .withArtifact( Workspace.RUN_SCRIPT, "macro/run-report-benchmark.sh" )
                                       .build();
        // when
        String json = JsonUtil.serializeJson( workspace );
        // then
        assertThat( JsonUtil.deserializeJson( json, Workspace.class ).allArtifacts(), equalTo( workspace.allArtifacts() ) );
    }
}
