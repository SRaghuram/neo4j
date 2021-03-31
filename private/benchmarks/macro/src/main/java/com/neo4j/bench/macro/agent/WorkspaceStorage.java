/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.agent;

import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.Extractor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;

public class WorkspaceStorage
{
    private final ArtifactStorage artifactStorage;
    private final Path workspace;

    public WorkspaceStorage( ArtifactStorage artifactStorage, Path workspace )
    {
        this.artifactStorage = artifactStorage;
        this.workspace = workspace;
    }

    public Path resolve( URI artifactUri )
    {
        return tryLocalFile( artifactUri )
                .orElseGet( () -> downloadCompressed( artifactUri ) );
    }

    private Optional<Path> tryLocalFile( URI uri )
    {
        if ( uri.getScheme() == null )
        {
            return Optional.of( Paths.get( uri.toString() ) );
        }
        if ( uri.getScheme().equals( "file" ) )
        {
            return Optional.of( Paths.get( uri ) );
        }
        return Optional.empty();
    }

    private Path downloadCompressed( URI datasetUri )
    {
        Path downloaded = download( datasetUri );
        return extract( workspace, downloaded );
    }

    private Path download( URI uri )
    {
        try
        {
            return artifactStorage.downloadSingleFile( workspace, uri );
        }
        catch ( ArtifactStoreException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Path extract( Path workspace, Path path )
    {
        try
        {
            Set<Path> topLevelPaths;
            try ( InputStream inputSteam = Files.newInputStream( path ) )
            {
                topLevelPaths = Extractor.extract( workspace, inputSteam );
            }
            Files.delete( path );
            if ( topLevelPaths.size() != 1 )
            {
                throw new RuntimeException( "Archive should contain exactly one top-level file, found: " + topLevelPaths );
            }
            return topLevelPaths.iterator().next();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
