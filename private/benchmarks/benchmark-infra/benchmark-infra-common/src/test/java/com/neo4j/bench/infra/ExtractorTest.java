/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class ExtractorTest
{

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void extractDataset() throws Exception
    {
        // given
        Path testArchive = createDatasetArchive();
        Path tempDir = temporaryFolder.newFolder( "neo" ).toPath();

        // when
        Extractor.extract( tempDir, Files.newInputStream( testArchive ) );

        // then
        assertTrue( Files.isRegularFile( tempDir.resolve( "neo.txt" ) ) );
    }

    private Path createDatasetArchive() throws IOException, CompressorException, ArchiveException
    {
        Path tempDataFile = temporaryFolder.newFile( "neofile.txt" ).toPath();
        Files.write( tempDataFile, Arrays.asList( "neo" ) );

        Path tempArchiveFile = temporaryFolder.newFile( "neo.tar.gz" ).toPath();

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
