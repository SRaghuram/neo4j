/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Version;

import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Describes structure of benchmarking workspace, which contains build artifacts.
 * Workspace has a base directory and a set of build artifacts (e.g., benchmarks jar or run scripts).
 */
public class Workspace
{
    public static final String JOB_PARAMETERS_JSON = "job-parameters.json";
    public static final String WORKSPACE_STRUCTURE_JSON = "workspace-structure.json";

    public static Workspace defaultMacroWorkspace( Path workspaceDir, Version neo4jVersion, Edition neo4jEdition )
    {
        return Workspace
                .create( workspaceDir )
                .withArtifacts(
                        // required artifacts
                        "neo4j.conf",
                        "benchmark-infra-worker.jar",
                        format( "neo4j-%s-%s-unix.tar.gz", neo4jEdition.name().toLowerCase(), neo4jVersion.fullVersion() ),
                        "macro/target/macro.jar",
                        "macro/run-report-benchmarks.sh",
                        JOB_PARAMETERS_JSON,
                        WORKSPACE_STRUCTURE_JSON
                ).build();
    }

    public static Workspace defaultMicroWorkspace( Path workspacePath )
    {
        return Workspace.create( workspacePath )
                        .withArtifacts(
                                // required artifacts
                                "neo4j.conf",
                                "benchmark-infra-worker.jar",
                                "micro/target/micro-benchmarks.jar",
                                "run-report-benchmarks.sh",
                                "micro.config",
                                JOB_PARAMETERS_JSON )
                        .build();
    }

    public static Workspace defaultMacroServerWorkspace( Path workspaceDir, String neo4jPath )
    {
        return Workspace
                .create( workspaceDir )
                .withArtifacts(
                        // required artifacts
                        "neo4j.conf",
                        "benchmark-infra-worker.jar",
                        format( "%s-unix.tar.gz", neo4jPath ),
                        "macro/target/macro.jar",
                        "macro/run-report-benchmarks.sh",
                        JOB_PARAMETERS_JSON,
                        WORKSPACE_STRUCTURE_JSON
                              ).build();
    }

    public static void assertMacroWorkspace( Workspace artifactsWorkspace, Version neo4jVersion, Edition neo4jEdition )
    {
        Workspace defaultMacroWorkspace = defaultMacroWorkspace( artifactsWorkspace.baseDir, neo4jVersion, neo4jEdition );
        assertWorkspaceAreEqual( defaultMacroWorkspace, artifactsWorkspace );
    }

    public static void assertWorkspaceAreEqual( Workspace original, Workspace newWorkspace )
    {
        if ( !newWorkspace.allArtifacts.containsAll( original.allArtifacts ) )
        {
            throw new IllegalArgumentException( String.format( "workspace doesn't contain all required paths. Expected: %s But got: %s", original.allArtifacts,
                                                               newWorkspace.allArtifacts ) );
        }
    }

    public Path get( String workspacePath )
    {
        Path path = baseDir.resolve( Paths.get( workspacePath ) );
        if ( !allArtifacts.contains( workspacePath ) )
        {
            throw new RuntimeException( format( "path %s not found in workspace %s", workspacePath, baseDir ) );
        }
        return path;
    }

    public static class Builder
    {
        private final Path baseDir;
        private final List<String> artifacts = new ArrayList<>();
        private FileFilter fileFilter;

        private Builder( Path baseDir )
        {
            this.baseDir = baseDir;
        }

        public Builder withArtifacts( String... artifacts )
        {
            Objects.requireNonNull( artifacts, "build artifacts cannot be null" );
            this.artifacts.addAll( Arrays.asList( artifacts ) );
            return this;
        }

        public Builder withFilesRecursively( FileFilter fileFilter )
        {
            Objects.requireNonNull( fileFilter, "file filter cannot be null" );
            this.fileFilter = fileFilter;
            return this;
        }

        public Workspace build()
        {
            List<Path> allArtifacts = artifacts.stream()
                                               .map( baseDir::resolve )
                                               .collect( Collectors.toList() );

            List<Path> invalidArtifacts = allArtifacts.stream()
                                                      .filter( artifact -> !Files.isRegularFile( artifact ) )
                                                      .collect( Collectors.toList() );

            if ( !invalidArtifacts.isEmpty() )
            {
                String missingArtifacts = invalidArtifacts.stream().map( Path::toString ).collect( Collectors.joining( "," ) );
                throw new IllegalStateException( format( "missing artifacts: %s\n", missingArtifacts ) );
            }

            if ( fileFilter != null )
            {
                try
                {
                    FilteringFileVisitor fileVisitor = new FilteringFileVisitor( fileFilter, allArtifacts );
                    Files.walkFileTree( baseDir, fileVisitor );
                    allArtifacts.addAll( fileVisitor.paths() );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
            return new Workspace( baseDir, allArtifacts.stream()
                                                       .map( baseDir::relativize )
                                                       .map( Path::toString ).collect( Collectors.toList() ) );
        }

        private static class FilteringFileVisitor extends SimpleFileVisitor<Path>
        {
            private final FileFilter fileFilter;
            private final List<Path> allArtifacts;
            private final Set<Path> filteredPaths = new HashSet<>();

            FilteringFileVisitor( FileFilter fileFilter, List<Path> allArtifacts )
            {
                this.fileFilter = fileFilter;
                this.allArtifacts = allArtifacts;
            }

            @Override
            public FileVisitResult visitFile( Path path, BasicFileAttributes attrs )
            {
                if ( !allArtifacts.contains( path.toAbsolutePath() ) && fileFilter.accept( path.toFile() ) )
                {
                    filteredPaths.add( path );
                }
                return FileVisitResult.CONTINUE;
            }

            Set<Path> paths()
            {
                return filteredPaths;
            }
        }
    }

    public static Builder create( Path baseDir )
    {
        Objects.requireNonNull( baseDir, "workspace base path cannot be null" );
        return new Builder( baseDir );
    }

    private final Path baseDir;
    private final List<String> allArtifacts;

    private Workspace( Path baseDir, List<String> allArtifacts )
    {
        this.baseDir = baseDir;
        this.allArtifacts = allArtifacts;
    }

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Workspace()
    {
        this( null, null );
    }

    public static Path findNeo4jArchive( String tarName, Path workspaceDir )
    {
        String[] neo4jTars = workspaceDir.toFile().list( ( dir, name ) -> name.contains( tarName ) );
        if ( neo4jTars.length != 1 )
        {
            throw new RuntimeException(
                    format( "Found the wrong number of neo4j tar.gz only expect one but found %s at %s", neo4jTars.length,
                            Arrays.toString( neo4jTars ) ) );
        }
        return workspaceDir.resolve( neo4jTars[0] );
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
        return allArtifacts.stream().map( baseDir::resolve ).collect( Collectors.toList() );
    }
}
