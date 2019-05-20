/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkspaceTest
{
    @Test
    void openExistingWorkspace() throws Exception
    {
        // given
        Path workspaceBaseDir = Files.createTempDirectory( "workspace" );
        // macro workspace structure
        Files.createFile( workspaceBaseDir.resolve( "benchmark-infra-scheduler.jar" ) );
        Files.createFile( workspaceBaseDir.resolve( "neo4j-enterprise-3.3.10-unix.tar.gz" ) );
        Files.createDirectories( workspaceBaseDir.resolve( "macro/target" ) );
        Files.createFile( workspaceBaseDir.resolve( "macro/target/macro.jar" ) );
        Files.createFile( workspaceBaseDir.resolve( "macro/run-report-benchmark.sh" ) );
        Workspace workspace = Workspace.create( workspaceBaseDir )
                .withArtifacts(
                    Paths.get( "benchmark-infra-scheduler.jar" ),
                    Paths.get( "neo4j-enterprise-3.3.10-unix.tar.gz" ),
                    Paths.get( "macro/target/macro.jar" ),
                    Paths.get( "macro/run-report-benchmark.sh" )
                ).build();

        assertNotNull( workspace );
    }

    @Test
    void throwExceptionIfWorkspaceIsEmpty() throws Exception
    {
        // given
        Path workspaceBaseDir = Files.createTempDirectory( "workspace" );
        // when, workspace is empty
        assertThrows( IllegalStateException.class, () ->
        {
            Workspace.create( workspaceBaseDir )
                     .withArtifacts(
                        Paths.get( "benchmark-infra-scheduler.jar" ),
                        Paths.get( "neo4j-enterprise-3.3.10-unix.tar.gz" ),
                        Paths.get( "macro/target/macro.jar" ),
                        Paths.get( "macro/run-report-benchmark.sh" )
                     ).build();
        });
    }
}
