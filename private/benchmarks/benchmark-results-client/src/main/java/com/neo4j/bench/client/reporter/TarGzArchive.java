/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class TarGzArchive
{

    private static final Logger LOG = LoggerFactory.getLogger( TarGzArchive.class );

    public static void compress( Path archivePath, Path directory ) throws IOException
    {
        try ( TarArchiveOutputStream archiveOutput = new TarArchiveOutputStream(
                new GzipCompressorOutputStream( new FileOutputStream( archivePath.toFile() ) ) );
              Stream<Path> paths = Files.walk( directory ) )
        {
            // this is required to handle long profiler recordings file names
            archiveOutput.setLongFileMode( TarArchiveOutputStream.LONGFILE_POSIX );
            paths.forEach(
                    path ->
                    {
                        try
                        {
                            ArchiveEntry archiveEntry = archiveOutput.createArchiveEntry( path.toFile(), directory.relativize( path ).toString() );
                            archiveOutput.putArchiveEntry( archiveEntry );
                            if ( Files.isRegularFile( path ) )
                            {
                                try ( InputStream inputStream = Files.newInputStream( path ) )
                                {
                                    IOUtils.copy( inputStream, archiveOutput );
                                }
                            }
                            archiveOutput.closeArchiveEntry();
                        }
                        catch ( IOException e )
                        {
                            throw new UncheckedIOException( e );
                        }
                    }
            );
        }
    }

    public static void extract( Path archive, Path directory ) throws IOException
    {
        try ( TarArchiveInputStream archiveInput = new TarArchiveInputStream( new GzipCompressorInputStream( new FileInputStream( archive.toFile() ) ) ) )
        {
            ArchiveEntry archiveEntry;
            while ( (archiveEntry = archiveInput.getNextEntry()) != null )
            {
                Path entryPath = directory.resolve( archiveEntry.getName() );
                if ( archiveEntry.isDirectory() )
                {
                    Files.createDirectories( entryPath );
                }
                else
                {
                    Files.createDirectories( entryPath.getParent() );
                    try ( OutputStream outputStream = Files.newOutputStream( entryPath ) )
                    {
                        IOUtils.copy( archiveInput, outputStream );
                    }
                }
            }
        }
    }
}
