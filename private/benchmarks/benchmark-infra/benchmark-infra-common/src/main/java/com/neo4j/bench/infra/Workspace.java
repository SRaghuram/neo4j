/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.options.Edition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Describes structure of benchmarking workspace, which contains build artifacts.
 * Workspace has a base directory and a set of build artifacts (e.g., benchmarks jar or run scripts).
 */
public class Workspace
{
    public static Workspace assertMacroWorkspace( Path workspaceDir, Edition neo4jEdition, String neo4jVersion )
    {
        return Workspace
                .create( workspaceDir.toAbsolutePath() )
                .withArtifacts(
                        // required artifacts
                        Paths.get( "neo4j.conf" ),
                        Paths.get( "benchmark-infra-worker.jar" ),
                        Paths.get( format( "neo4j-%s-%s-unix.tar.gz", neo4jEdition.name().toLowerCase(), neo4jVersion ) ),
                        Paths.get( "macro/target/macro.jar" ),
                        Paths.get( "macro/run-report-benchmarks.sh" )
                ).build();
    }

    public static class Builder
    {
        private final Path baseDir;
        private final List<Path> artifacts = new ArrayList<>();

        private Builder( Path baseDir )
        {
            this.baseDir = baseDir;
        }

        public Builder withArtifacts( Path... artifacts )
        {
            Objects.requireNonNull( artifacts, "build artifacts cannot be null" );
            this.artifacts.addAll( Arrays.asList( artifacts ) );
            return this;
        }

        public Workspace build()
        {
            List<Path> allArtifacts = artifacts.stream()
                                               .map( baseDir::resolve )
                                               .map( Path::toAbsolutePath )
                                               .collect( Collectors.toList() );

            List<Path> invalidArtifacts = allArtifacts.stream()
                                                      .filter( artifact -> !Files.isRegularFile( artifact ) )
                                                      .collect( Collectors.toList() );

            if ( !invalidArtifacts.isEmpty() )
            {
                String missingArtifacts = invalidArtifacts.stream().map( Path::toString ).collect( Collectors.joining( "," ) );
                throw new IllegalStateException( format( "missing artifacts: %s\n", missingArtifacts ) );
            }

            return new Workspace( baseDir, allArtifacts );
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger( Workspace.class );

    public static Builder create( Path baseDir )
    {
        Objects.requireNonNull( baseDir, "workspace base path cannot be null" );
        return new Builder( baseDir );
    }

    private final Path baseDir;
    private final List<Path> allArtifacts;

    private Workspace( Path baseDir, List<Path> allArtifacts )
    {
        this.baseDir = baseDir;
        this.allArtifacts = allArtifacts;
    }

    /**
     * Workspace's base dir.
     * All artifacts are relative to base dir.
     *
     * @return
     */
    public Path baseDir()
    {
        return baseDir;
    }

    /**
     * All build artifacts in this workspace (like benchmark jars and run scripts)
     *
     * @return
     */
    public List<Path> allArtifacts()
    {
        return allArtifacts;
    }

    public boolean isValid( Path anotherBaseDir )
    {
        allArtifacts.stream()
                    .map( baseDir::relativize )
                    .map( anotherBaseDir::resolve ).forEach( p -> {
                                                                    System.out.println( p.getFileName() );
                                                                    System.out.println( Files.isRegularFile( p ) );
        } );
        return allArtifacts.stream()
                           .map( baseDir::relativize )
                           .map( anotherBaseDir::resolve )
                           .allMatch( Files::isRegularFile );
    }
}
