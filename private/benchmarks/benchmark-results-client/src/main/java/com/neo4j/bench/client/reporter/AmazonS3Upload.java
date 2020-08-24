/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.reporter;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class AmazonS3Upload implements AutoCloseable
{

    private static final Logger LOG = LoggerFactory.getLogger( AmazonS3Upload.class );

    public static AmazonS3Upload create( String awsRegion, String awsEndpointURL )
    {
        LOG.debug( format( "creating Amazon S3 upload client with region '%s' and AWS endpoint '%s'", awsRegion, awsEndpointURL ) );
        try
        {
            return new AmazonS3Upload( createAmazonS3Client( awsEndpointURL != null ? new URL( awsEndpointURL ) : null, awsRegion ) );
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException( format( "unable to parse endpoint URL '%s'", awsEndpointURL ), e );
        }
    }

    private static AmazonS3 createAmazonS3Client( URL endpointUrl, String region )
    {
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3Client.builder()
                                                                    .withCredentials( DefaultAWSCredentialsProviderChain.getInstance() );
        if ( endpointUrl != null )
        {
            LOG.debug( format( "S3 client endpoint URL '%s' at region '%s'", endpointUrl, region ) );
            amazonS3ClientBuilder =
                    amazonS3ClientBuilder.withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration( endpointUrl.toString(), region ) );
        }
        else
        {
            amazonS3ClientBuilder = amazonS3ClientBuilder.withRegion( region );
        }
        return amazonS3ClientBuilder.enablePathStyleAccess().build();
    }

    private static void awaitUpload( Upload upload )
    {
        try
        {
            LOG.debug( format( "waiting for upload %s completion", upload ) );
            upload.waitForCompletion();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static String getS3Path( String fullPath )
    {
        return removeStart( fullPath, "/" );
    }

    private final AmazonS3 amazonS3;

    public AmazonS3Upload( AmazonS3 amazonS3 )
    {
        this.amazonS3 = amazonS3;
    }

    public void uploadFile( Path source, URI destination ) throws IOException
    {
        String bucketName = destination.getAuthority();
        String s3key = getS3Path( destination.getPath() );
        LOG.debug( format( "uploading file '%s' to bucket '%s' at key '%s'", source, bucketName, s3key ) );
        PutObjectRequest putObjectRequest = new PutObjectRequest( bucketName, s3key, source.toFile() );
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength( Files.size( source ) );
        amazonS3.putObject( putObjectRequest.withMetadata( objectMetadata ) );
    }

    public void uploadFolder( Path source, URI destination )
    {
        String bucketName = destination.getAuthority();
        Path s3key = Paths.get( getS3Path( destination.getPath() ) );

        try ( Stream<Path> paths = Files.walk( source ) )
        {
            TransferManager transferManager = TransferManagerBuilder.standard().withS3Client( amazonS3 ).build();
            try
            {
                paths.filter( Files::isRegularFile )
                     .map( sourcePath ->
                           {
                               Path destinationPath = s3key.resolve( source.relativize( sourcePath ) );
                               LOG.debug( format( "upload '%s' to '%s'", sourcePath, destinationPath ) );
                               try
                               {
                                   ObjectMetadata objectMetadata = new ObjectMetadata();
                                   objectMetadata.setContentLength( Files.size( sourcePath ) );
                                   PutObjectRequest putObjectRequest = new PutObjectRequest( bucketName,
                                                                                             destinationPath.toString(),
                                                                                             sourcePath.toFile() );
                                   return transferManager.upload( putObjectRequest.withMetadata( objectMetadata ) );
                               }
                               catch ( IOException e )
                               {
                                   throw new UncheckedIOException( e );
                               }
                           } )
                     .forEach( AmazonS3Upload::awaitUpload );
            }
            finally
            {
                transferManager.shutdownNow( false );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void close() throws Exception
    {
        amazonS3.shutdown();
    }
}
