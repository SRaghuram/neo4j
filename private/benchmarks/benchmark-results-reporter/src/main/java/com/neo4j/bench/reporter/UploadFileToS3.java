/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.reporter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class UploadFileToS3
{
    public static URI execute( AmazonS3Upload amazonS3Upload, String bucketName, String keyPrefix, Path file )
    {
        try
        {
            URI fileURI = new URI( format( "s3://%s/%s/%s",
                                           bucketName,
                                           removeEnd( removeStart( keyPrefix, "/" ), "/" ),
                                           file.getFileName() ) );
            System.out.println( format( "uploading file %s to %s", file, fileURI ) );
            amazonS3Upload.uploadFile( file, fileURI );
            return fileURI;
        }
        catch ( URISyntaxException | IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
