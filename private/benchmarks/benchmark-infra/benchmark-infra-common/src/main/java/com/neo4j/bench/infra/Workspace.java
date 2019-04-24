/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.amazonaws.util.IOUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.BufferedInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Describes structure of benchmarking workspace,
 * in terms of files and directories
 *
 */
public class Workspace
{

    public static Workspace create( Path baseDir, Path... requiredArtifacts )
    {

        List<Path> allArtifacts = Arrays.stream( requiredArtifacts )
                .map( artifact -> baseDir.resolve( artifact ) )
                .map( Path::toAbsolutePath )
                .collect( Collectors.toList() );

        List<Path> invalidArtifacts = allArtifacts.stream()
                .filter( artifact -> !Files.isRegularFile( artifact ) )
                .collect( Collectors.toList() );

        if ( !invalidArtifacts.isEmpty() )
        {
            String missingArtifacts = invalidArtifacts.stream().map( Path::toString ).collect( Collectors.joining( "," ) );
            throw new IllegalStateException( String.format( "missing artifacts: %s\n", missingArtifacts ) );
        }

        return new Workspace( baseDir, allArtifacts );
    }

    private final Path baseDir;
    private final List<Path> allArtifacts;

    Workspace( Path baseDir, List<Path> allArtifacts )
    {
        this.baseDir = baseDir;
        this.allArtifacts = allArtifacts;
    }

    public Path baseDir()
    {
        return baseDir;
    }

    public List<Path> allArtifacts()
    {
        return allArtifacts;
    }

    public boolean isValid( Path anotherBaseDir )
    {
        return allArtifacts.stream()
            .map( artifact -> baseDir.relativize( artifact) )
            .map( artifact -> anotherBaseDir.resolve( artifact ) )
            .filter( artifact -> !Files.isRegularFile( artifact ) )
            .count() == 0;
    }

    public void extractNeo4jConfig( String neo4jVersion, Path neo4jConfig )
    {
        Path productArchive = baseDir.resolve( format( "neo4j-enterprise-%s-unix.tar.gz", neo4jVersion ) );
        if ( !Files.isRegularFile( productArchive ) )
        {
            throw new IllegalStateException( format( "cannot find product archive at %s", productArchive ) );
        }
        try ( InputStream objectContent = new BufferedInputStream( Files.newInputStream( productArchive ) );
              InputStream compressorInput = new CompressorStreamFactory()
                      .createCompressorInputStream( CompressorStreamFactory.GZIP, objectContent );
              ArchiveInputStream archiveInput =
                      new ArchiveStreamFactory().createArchiveInputStream( ArchiveStreamFactory.TAR, compressorInput ) )
        {
            ArchiveEntry entry = null;
            while ( (entry = archiveInput.getNextEntry()) != null )
            {
                if ( !archiveInput.canReadEntryData( entry ) )
                {
                    // log something?
                    continue;
                }
                if ( !entry.isDirectory() && entry.getName().endsWith( "neo4j.conf" ) )
                {
                    IOUtils.copy( archiveInput, Files.newOutputStream( neo4jConfig ) );
                    return;
                }
            }
        }
        catch ( IOException | CompressorException | ArchiveException e )
        {
            throw new IOError( e );
        }
        throw new RuntimeException( format( "neo4j.conf not found in %s", productArchive ) );
    }
}
