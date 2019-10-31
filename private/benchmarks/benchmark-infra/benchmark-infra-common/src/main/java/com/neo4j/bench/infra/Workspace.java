/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
    public static Workspace assertMacroWorkspace( Path workspaceDir, Edition neo4jEdition, Version neo4jVersion )
    {
        return Workspace
                .create( workspaceDir.toAbsolutePath() )
                .withArtifacts(
                        // required artifacts
                        Paths.get( "neo4j.conf" ),
                        Paths.get( "benchmark-infra-worker.jar" ),
                        Paths.get( format( "neo4j-%s-%s-unix.tar.gz", neo4jEdition.name().toLowerCase(), neo4jVersion.patchVersion() ) ),
                        Paths.get( "macro/target/macro.jar" ),
                        Paths.get( "macro/run-report-benchmarks.sh" )
                ).build();
    }

    public static class Builder
    {
        private final Path baseDir;
        private final List<Path> artifacts = new ArrayList<>();
        private FileFilter fileFilter;

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

            return new Workspace( baseDir, allArtifacts );
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
        return allArtifacts.stream()
                           .map( baseDir::relativize )
                           .map( anotherBaseDir::resolve )
                           .allMatch( Files::isRegularFile );
    }
}
