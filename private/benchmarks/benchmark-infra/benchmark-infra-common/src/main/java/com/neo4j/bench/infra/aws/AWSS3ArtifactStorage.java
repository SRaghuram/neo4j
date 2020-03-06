/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.appendIfMissing;
import static org.apache.commons.lang3.StringUtils.prependIfMissing;
import static org.apache.commons.lang3.StringUtils.removeStart;

public class AWSS3ArtifactStorage implements ArtifactStorage
{

    private static final Logger LOG = LoggerFactory.getLogger( AWSS3ArtifactStorage.class );

    static final String BENCHMARKING_BUCKET_NAME = "benchmarking.neo4j.com";

    public static AWSS3ArtifactStorage create( String region, String awsKey, String awsSecret )
    {
        AWSStaticCredentialsProvider credentialsProvider =
                new AWSStaticCredentialsProvider( new BasicAWSCredentials( awsKey, awsSecret ) );
        return new AWSS3ArtifactStorage( s3ClientBuilder( credentialsProvider ).withRegion( region ).build() );
    }

    public static AWSS3ArtifactStorage create( EndpointConfiguration endpointConfiguration )
    {
        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        return new AWSS3ArtifactStorage( s3ClientBuilder( credentialsProvider ).withEndpointConfiguration( endpointConfiguration ).build() );
    }

    public static AWSS3ArtifactStorage create( String awsRegion )
    {
        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        return new AWSS3ArtifactStorage( s3ClientBuilder( credentialsProvider ).withRegion( awsRegion ).build() );
    }

    public static AWSS3ArtifactStorage getAWSS3ArtifactStorage( InfraParams infraParams )
    {
        if ( infraParams.hasAwsCredentials() )
        {
            return create( infraParams.awsRegion(),
                           infraParams.awsKey(),
                           infraParams.awsSecret() );
        }
        else
        {
            return create( infraParams.awsRegion() );
        }
    }

    private static AmazonS3ClientBuilder s3ClientBuilder( AWSCredentialsProvider credentialsProvider )
    {
        LOG.debug( "connecting to Amazon S3" );
        return AmazonS3Client.builder()
                             .withCredentials( credentialsProvider )
                             .withPathStyleAccessEnabled( true );
    }

    private final AmazonS3 amazonS3;

    AWSS3ArtifactStorage( AmazonS3 amazonS3 )
    {
        this.amazonS3 = amazonS3;
    }

    @Override
    public void uploadBuildArtifacts( URI artifactBaseURI, Workspace workspace ) throws ArtifactStoreException
    {
        LOG.debug( "uploading build {} artifacts from workspace {}", artifactBaseURI, workspace.baseDir() );
        String bucketName = artifactBaseURI.getAuthority();
        String s3Path = getS3Path( artifactBaseURI.getPath() );
        if ( !amazonS3.doesBucketExistV2( bucketName ) )
        {
            throw new RuntimeException( format( "the bucket %s does not exists, please create it.", bucketName ) );
        }
        try
        {
            TransferManager tm = TransferManagerBuilder.standard()
                                                       .withS3Client( amazonS3 )
                                                       .build();
            List<Upload> uploads = new ArrayList<>();

            for ( Path artifact : workspace.allArtifacts() )
            {
                String s3key = Paths.get( s3Path, workspace.baseDir().relativize( artifact ).toString() ).toString();
                LOG.info( "upload artifact {} to path {}",
                          artifact.toString(),
                          new URI( artifactBaseURI.getScheme(), artifactBaseURI.getHost(), prependIfMissing( s3key, "/" ), null ) );
                // TransferManager processes all transfers asynchronously,
                // so this call returns immediately.
                PutObjectRequest putObjectRequest = new PutObjectRequest( bucketName, s3key, artifact.toFile() );
                ObjectMetadata objectMetadata = new ObjectMetadata();
                objectMetadata.setContentLength( Files.size( artifact ) );
                uploads.add( tm.upload( putObjectRequest.withMetadata( objectMetadata ) ) );
                LOG.info( "Object upload started" );
            }
            for ( Upload upload : uploads )
            {
                upload.waitForCompletion();
            }
            LOG.info( "All object upload complete" );
            tm.shutdownNow( false );
        }
        catch ( URISyntaxException | InterruptedException | IOException e )
        {
            throw new ArtifactStoreException( e );
        }
    }

    @Override
    public Workspace downloadBuildArtifacts( Path baseDir, URI artifactBaseURI ) throws ArtifactStoreException
    {
        String bucketName = artifactBaseURI.getAuthority();
        String s3Path = getS3Path( artifactBaseURI.getPath() );
        LOG.info( "downloading build artifacts from bucket {} at key prefix {}", bucketName, s3Path );

        ObjectListing objectListing = amazonS3.listObjects( bucketName, s3Path );
        try
        {
            Workspace.Builder builder = Workspace.create( baseDir );
            for ( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() )
            {
                String key = objectSummary.getKey();
                String relativeArtifact = key.replace( s3Path, "" );
                S3Object s3Object = amazonS3.getObject( bucketName, key );
                Path absoluteArtifact = baseDir.resolve( relativeArtifact );
                Files.createDirectories( absoluteArtifact.getParent() );
                LOG.info( "copying build artifact {} into {}", key, absoluteArtifact );
                Files.copy( s3Object.getObjectContent(), absoluteArtifact, StandardCopyOption.REPLACE_EXISTING );
                builder.withArtifacts( baseDir.relativize( absoluteArtifact ).toString() );
            }
            return builder.build();
        }
        catch ( IOException e )
        {
            throw new ArtifactStoreException( e );
        }
    }

    @Override
    public Dataset downloadDataset( String neo4jVersion, String dataset )
    {
        String key = createDatasetKey( neo4jVersion, dataset );
        LOG.info( "downloading dataset {} for version {} from bucket {} at key {}", dataset, neo4jVersion, BENCHMARKING_BUCKET_NAME, key );
        S3Object s3Object = amazonS3.getObject( BENCHMARKING_BUCKET_NAME, key );
        return new S3ObjectDataset( s3Object );
    }

    private static String getS3Path( String fullPath )
    {
        return appendIfMissing( removeStart( fullPath, "/" ), "/" );
    }

    private static String createDatasetKey( String neo4jVersion, String dataset )
    {
        return format( "datasets/macro/%s-enterprise-datasets/%s.tgz", neo4jVersion, dataset );
    }
}
