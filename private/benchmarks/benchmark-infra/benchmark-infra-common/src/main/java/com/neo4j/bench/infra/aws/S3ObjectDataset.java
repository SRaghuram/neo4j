/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

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
        LOG.info( "extracting dataset into {}", dir );
        Extractor.extract( dir, s3Object.getObjectContent() );
    }
}
