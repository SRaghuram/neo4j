/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class Compressor
{
    public static void compress( Path archivePath, Path directory )
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
                                    org.apache.commons.compress.utils.IOUtils.copy( inputStream, archiveOutput );
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
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
