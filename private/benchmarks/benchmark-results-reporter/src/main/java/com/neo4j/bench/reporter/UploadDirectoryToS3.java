/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.reporter;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class UploadDirectoryToS3
{

    public static URI execute( AmazonS3Upload amazonS3Upload, String bucketName, String keyPrefix, File profilerRecordingsOutputDir )
    {
        try
        {
            URI destinationURI = new URI( format( "s3://%s/%s/%s",
                                                  bucketName,
                                                  removeEnd( removeStart( keyPrefix, "/" ), "/" ),
                                                  profilerRecordingsOutputDir.getName() ) );
            System.out.println( format( "uploading recordings %s to %s", profilerRecordingsOutputDir, destinationURI ) );
            amazonS3Upload.uploadFolder( profilerRecordingsOutputDir.toPath(), destinationURI );
            return destinationURI;
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }
}
