/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.agent;

import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.Extractor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public class WorkspaceStorage
{
    private final ArtifactStorage artifactStorage;
    private final Path workspace;
    private final URI artifactBaseUri;
    private final String productArchive;
    private final URI dataSetBaseUri;
    private final String datasetName;
    private final Version version;

    public WorkspaceStorage( ArtifactStorage artifactStorage,
                             Path workspace,
                             URI artifactBaseUri,
                             String productArchive,
                             URI dataSetBaseUri,
                             String datasetName,
                             Version version )
    {
        this.artifactStorage = artifactStorage;
        this.workspace = workspace;
        this.artifactBaseUri = artifactBaseUri;
        this.productArchive = productArchive;
        this.dataSetBaseUri = dataSetBaseUri;
        this.datasetName = datasetName;
        this.version = version;
    }

    public WorkspaceState download()
    {
        Path neo4jTar = downloadArtifact();
        extract( workspace, neo4jTar );

        Dataset dataset = downloadDataset();
        dataset.extractInto( workspace );

        return new WorkspaceState( workspace.resolve( datasetName ), workspace.resolve( productArchive.replaceAll( "\\.(tgz|tar\\.gz)$", "" ) ) );
    }

    private Dataset downloadDataset()
    {
        return artifactStorage.downloadDataset( dataSetBaseUri, version.minorVersion(), datasetName );
    }

    private void extract( Path workspace, Path neo4jTar )
    {
        try
        {
            try ( InputStream inputSteam = Files.newInputStream( neo4jTar ) )
            {
                Extractor.extract( workspace, inputSteam );
            }
            Files.delete( neo4jTar );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private Path downloadArtifact()
    {
        try
        {
            return artifactStorage.downloadSingleFile( productArchive, workspace, artifactBaseUri );
        }
        catch ( ArtifactStoreException e )
        {
            throw new RuntimeException( e );
        }
    }
}
