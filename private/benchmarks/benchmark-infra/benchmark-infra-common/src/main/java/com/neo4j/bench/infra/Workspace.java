/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * Describes structure of benchmarking workspace, which contains build artifacts.
 * <p>
 * Workspace has a base directory and a set of build artifacts (e.g., benchmarks jar or run scripts).
 */
public class Workspace
{
    public static final String JOB_PARAMETERS_JSON = "job-parameters.json";

    public static final String NEO4J_CONFIG = "neo4j_config";
    public static final String BENCHMARKING_CONFIG = "benchmarking_config";
    public static final String WORKER_JAR = "worker_jar";
    public static final String BENCHMARKING_JAR = "tool_jar";
    public static final String RUN_SCRIPT = "run_script";
    public static final String NEO4J_ARCHIVE = "neo4j_archive";

    public static Workspace defaultMacroEmbeddedWorkspace( Path workspaceDir )
    {
        return Workspace
                .create( workspaceDir )
                .withArtifact( NEO4J_CONFIG, "neo4j.conf" )
                .withArtifact( WORKER_JAR, "benchmark-infra-worker.jar" )
                .withArtifact( BENCHMARKING_JAR, "macro/target/macro.jar" )
                .withArtifact( RUN_SCRIPT, "macro/run-report-benchmarks.sh" )
                .withArtifact( JOB_PARAMETERS_JSON, JOB_PARAMETERS_JSON )
                .build();
    }

    public static Workspace defaultMacroServerWorkspace( Path workspaceDir, String neo4jPath )
    {
        return Workspace
                .create( workspaceDir )
                .withArtifact( NEO4J_CONFIG, "neo4j.conf" )
                .withArtifact( WORKER_JAR, "benchmark-infra-worker.jar" )
                .withArtifact( NEO4J_ARCHIVE, format( "%s-unix.tar.gz", neo4jPath ) )
                .withArtifact( BENCHMARKING_JAR, "macro/target/macro.jar" )
                .withArtifact( RUN_SCRIPT, "macro/run-report-benchmarks.sh" )
                .withArtifact( JOB_PARAMETERS_JSON, JOB_PARAMETERS_JSON )
                .build();
    }

    public static Workspace defaultMicroWorkspace( Path workspacePath )
    {
        return Workspace
                .create( workspacePath )
                .withArtifact( NEO4J_CONFIG, "neo4j.conf" )
                .withArtifact( BENCHMARKING_CONFIG, "config" )
                .withArtifact( WORKER_JAR, "benchmark-infra-worker.jar" )
                .withArtifact( BENCHMARKING_JAR, "micro/target/micro-benchmarks.jar" )
                .withArtifact( RUN_SCRIPT, "micro/run-report-benchmarks.sh" )
                .withArtifact( JOB_PARAMETERS_JSON, JOB_PARAMETERS_JSON )
                .build();
    }

    /**
     * Asserts that <code>newWorkspace</code> has the same keys defined as <code>otherDefinition</code>.
     * <p>
     * Additionally asserts that for every key defined in <code>newWorkspace</code> the corresponding file exists.
     */
    public static void assertWorkspaceAreEqual( Workspace otherDefinition, Workspace newWorkspace )
    {
        if ( !newWorkspace.allArtifacts.keySet().equals( otherDefinition.allArtifacts.keySet() ) )
        {
            throw new IllegalArgumentException( "workspace doesn't contain all required paths" );
        }
        newWorkspace.assertArtifactsExist();
    }

    public void assertArtifactsExist()
    {
        allArtifacts.values().forEach( relativePathString -> Files.exists( Paths.get( baseDir.toString(), relativePathString ) ) );
    }

    public static void assertMicroWorkspace( Workspace artifactsWorkspace )
    {
        Workspace defaultMacroWorkspace = defaultMicroWorkspace( artifactsWorkspace.baseDir );

        if ( !artifactsWorkspace.allArtifacts().containsAll( defaultMacroWorkspace.allArtifacts() ) )
        {
            throw new IllegalArgumentException(
                    "workspace doesn't contain all required paths. Expected: "
                    + artifactsWorkspace.allArtifacts()
                    + " But got: " +
                    defaultMacroWorkspace.allArtifacts() );
        }
    }

    public Path get( String key )
    {

        if ( !allArtifacts.containsKey( key ) )
        {
            throw new RuntimeException( format( "key %s not found in workspace %s", key, baseDir ) );
        }
        return baseDir.resolve( allArtifacts.get( key ) );
    }

    public static class Builder
    {
        private final Path baseDir;
        private final Map<String,String> artifacts;
        private FileFilter fileFilter;

        private Builder( Path baseDir )
        {
            this.baseDir = baseDir;
            this.artifacts = new HashMap<>();
            this.fileFilter = null;
        }

        public Builder withArtifact( String key, String file )
        {
            Objects.requireNonNull( file, "build artifacts cannot be null" );
            Objects.requireNonNull( key, "build artifacts cannot be null" );
            this.artifacts.put( key, file );
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
            if ( null != fileFilter )
            {
                try ( Stream<Path> dirs = Files.walk( baseDir ) )
                {
                    dirs.filter( dir -> fileFilter.accept( dir.toFile() ) )
                        .filter( Files::isRegularFile )
                        .map( baseDir::relativize )
                        .forEach( relativePath -> artifacts.put( relativePath.toString(), relativePath.toString() ) );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( "Encountered error while traversing work directory", e );
                }
            }
            ensureValidWorkspaceFiles();

            return new Workspace( baseDir, artifacts );
        }

        private void ensureValidWorkspaceFiles()
        {
            List<Path> invalidArtifacts = artifacts.values().stream()
                                                   .map( baseDir::resolve )
                                                   .filter( artifact -> !Files.isRegularFile( artifact ) )
                                                   .collect( toList() );

            if ( !invalidArtifacts.isEmpty() )
            {
                String missingArtifacts = invalidArtifacts.stream().map( Path::toString ).collect( Collectors.joining( "\n\t" ) );
                throw new IllegalStateException( format( "missing artifacts:\n\t%s", missingArtifacts ) );
            }
        }
    }

    public static Builder create( Path baseDir )
    {
        Objects.requireNonNull( baseDir, "workspace base path cannot be null" );
        return new Builder( baseDir );
    }

    private final Path baseDir;
    private final Map<String,String> allArtifacts;

    @JsonCreator
    private Workspace( @JsonProperty( "baseDir" ) Path baseDir, @JsonProperty( "allArtifacts" ) Map<String,String> allArtifacts )
    {
        this.baseDir = baseDir;
        this.allArtifacts = allArtifacts;
    }

    public String getString( String key )
    {
        return allArtifacts.get( key );
    }

    /**
     * Workspace's base dir. All artifacts are relative to base dir.
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
        return allArtifacts.values().stream().map( baseDir::resolve ).collect( toList() );
    }

    public List<String> allArtifactKeys()
    {
        return Lists.newArrayList( allArtifacts.keySet() );
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
