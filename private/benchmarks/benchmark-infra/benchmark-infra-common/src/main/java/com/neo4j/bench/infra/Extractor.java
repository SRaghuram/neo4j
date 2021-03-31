/*
 * Copyright (c) "Neo4j"
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;

public class Extractor
{
    private static final Logger LOG = LoggerFactory.getLogger( Extractor.class );

    public static Set<Path> extract( Path dir, InputStream inputSteam )
    {
        try ( InputStream objectContent = new BufferedInputStream( inputSteam );
              InputStream compressorInput = new CompressorStreamFactory()
                      .createCompressorInputStream( CompressorStreamFactory.GZIP, objectContent );
              ArchiveInputStream archiveInput =
                      new ArchiveStreamFactory().createArchiveInputStream( ArchiveStreamFactory.TAR, compressorInput ) )
        {
            Set<Path> topLevelPaths = new HashSet<>();
            ArchiveEntry entry;
            while ( (entry = archiveInput.getNextEntry()) != null )
            {
                if ( !archiveInput.canReadEntryData( entry ) )
                {
                    LOG.error( "can't read data entry {}", entry.getName() );
                    continue;
                }
                File f = dir.resolve( entry.getName() ).toFile();
                if ( Objects.equals( f.getParentFile(), dir.toFile() ) )
                {
                    topLevelPaths.add( f.toPath() );
                }
                LOG.debug( f.toString() );
                if ( entry.isDirectory() )
                {
                    mkDirs( f );
                }
                else
                {
                    File parent = f.getParentFile();
                    mkDirs( parent );
                    try ( OutputStream o = Files.newOutputStream( f.toPath() ) )
                    {
                        IOUtils.copy( archiveInput, o );
                    }
                }
            }
            return topLevelPaths;
        }
        catch ( IOException | CompressorException | ArchiveException e )
        {
            throw new IOError( e );
        }
    }

    private static void mkDirs( File f ) throws IOException
    {
        if ( !f.isDirectory() && !f.mkdirs() )
        {
            throw new IOException( format( "failed to create directory %s", f ) );
        }
    }
}
