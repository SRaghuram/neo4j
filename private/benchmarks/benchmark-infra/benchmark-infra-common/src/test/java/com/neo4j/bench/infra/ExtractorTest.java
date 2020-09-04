/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExtractorTest
{

    @Test
    public void extractDataset( @TempDir Path tempDir ) throws Exception
    {
        // given
        Path testArchive = createDatasetArchive( tempDir );
        Path dir = Files.createTempDirectory( tempDir, "neo" );

        // when
        Extractor.extract( dir, Files.newInputStream( testArchive ) );

        // then
        assertTrue( Files.isRegularFile( dir.resolve( "neo.txt" ) ) );
    }

    private Path createDatasetArchive( @TempDir Path tempDir ) throws IOException, CompressorException, ArchiveException
    {
        Path tempDataFile = Files.createTempFile( tempDir, "neofile", ".txt" );
        Files.write( tempDataFile, Arrays.asList( "neo" ) );

        Path tempArchiveFile = Files.createTempFile( tempDir, "neo", ".tar.gz" );

        try ( CompressorOutputStream compressorOutput = new CompressorStreamFactory()
                .createCompressorOutputStream( CompressorStreamFactory.GZIP, Files.newOutputStream( tempArchiveFile ) );
              ArchiveOutputStream archiveOutput =
                      new ArchiveStreamFactory().createArchiveOutputStream( ArchiveStreamFactory.TAR, compressorOutput ) )
        {
            ArchiveEntry archiveEntry = archiveOutput.createArchiveEntry( tempDataFile.toFile(), "neo.txt" );
            archiveOutput.putArchiveEntry( archiveEntry );
            IOUtils.copy( Files.newInputStream( tempDataFile ), archiveOutput );
            archiveOutput.closeArchiveEntry();
        }
        return tempArchiveFile;
    }
}
