/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.neo4j.bench.infra.Dataset;
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

import static java.lang.String.format;

class S3ObjectDataset implements Dataset
{

    private static final Logger LOG = LoggerFactory.getLogger( S3ObjectDataset.class );

    private final S3Object s3Object;

    S3ObjectDataset( S3Object s3Object )
    {
        this.s3Object = s3Object;
    }

    @Override
    public void copyInto( OutputStream output ) throws IOException
    {
        try ( InputStream input = s3Object.getObjectContent() )
        {
            IOUtils.copy( input, output );
        }
    }

    @Override
    public void extractInto( Path dir )
    {
        LOG.info( "extracting dataset into {}", dir  );
        try ( InputStream objectContent = new BufferedInputStream( s3Object.getObjectContent() );
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
                    LOG.error( "can't read data entry {}", entry.getName() );
                    continue;
                }
                File f = dir.resolve( entry.getName() ).toFile();
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
