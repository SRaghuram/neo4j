/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.GetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

import static java.util.Arrays.asList;

public class AWSS3ArtifactStorage implements ArtifactStorage
{

    private static final Logger LOG = LoggerFactory.getLogger( AWSS3ArtifactStorage.class );

    private static final String ARTIFACTS_PREFIX = "artifacts";
    private static final String RULE_ID = "expire all build artifacts";
    private static final int EXPIRATION_IN_DAYS = 7;

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
    public URI uploadBuildArtifacts( String buildID, Workspace workspace )
            throws URISyntaxException, AmazonServiceException, SdkClientException, IOException
    {
        LOG.debug( "uploading build {} artifacts from workspace {}", buildID, workspace.baseDir() );
        for ( Path artifact : workspace.allArtifacts() )
        {
            String key = createBuildArtifactKey( buildID, workspace.baseDir().relativize( artifact ) );

            ObjectMetadata objectMetadata = new ObjectMetadata();
            // don't you ever dare to touch it,
            // otherwise you will run out of memory
            // as AWS S3 client tries to cache whole stream in memory
            // if size is unknown
            objectMetadata.setContentLength( Files.size( artifact ) );

            LOG.info( "upload artifact {} with build id {}", artifact.toString(), buildID );

            PutObjectResult result = amazonS3.putObject( BENCHMARKING_BUCKET_NAME, key, Files.newInputStream( artifact ), objectMetadata );
            // TODO this fails under tests, and works with real implementation
            // Objects.requireNonNull( result.getExpirationTime(), "build artifacts should have expiration time set" );
        }

        String uriPath = Paths.get( "/", ARTIFACTS_PREFIX, buildID ).toString();
        return new URI( "s3", BENCHMARKING_BUCKET_NAME, uriPath, null );
    }

    @Override
    public void downloadBuildArtifacts( Path baseDir, String buildID ) throws IOException
    {
        String keyPrefix = createBuildArtifactPrefix( buildID );
        LOG.info( "downloading build artifacts for build {} from bucket {} at key prefix {}", buildID,  BENCHMARKING_BUCKET_NAME, keyPrefix );

        ObjectListing objectListing = amazonS3.listObjects( BENCHMARKING_BUCKET_NAME, keyPrefix );
        for ( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() )
        {
            String key = objectSummary.getKey();
            String relativeArtifact = key.replace( keyPrefix + "/", "" );
            S3Object s3Object = amazonS3.getObject( BENCHMARKING_BUCKET_NAME, key );
            Path absoluteArtifact = baseDir.resolve( relativeArtifact );
            Files.createDirectories( absoluteArtifact.getParent() );
            LOG.info( "copying build artifact {} into {}", key, absoluteArtifact );
            Files.copy( s3Object.getObjectContent(), absoluteArtifact, StandardCopyOption.REPLACE_EXISTING );
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

    private String createDatasetKey( String neo4jVersion, String dataset )
    {
        return String.format( "datasets/macro/%s-enterprise-datasets/%s.tgz", neo4jVersion, dataset );
    }

    private static String createBuildArtifactPrefix( String buildID )
    {
        return String.format( "%s/%s", ARTIFACTS_PREFIX, buildID );
    }

    public void verifyBuildArtifactsExpirationRule()
    {
        LOG.debug( "verifying build artifacts expiration rule" );
        Optional<BucketLifecycleConfiguration> lifecycleConfiguration = Optional.ofNullable( amazonS3.getBucketLifecycleConfiguration(
                new GetBucketLifecycleConfigurationRequest( BENCHMARKING_BUCKET_NAME ) ) );

        Rule rule = lifecycleConfiguration
                .map( BucketLifecycleConfiguration::getRules )
                .flatMap( rules -> rules.stream().filter( r -> RULE_ID.equals( r.getId() ) ).findFirst() )
                .orElseGet( () -> new BucketLifecycleConfiguration.Rule().withId( RULE_ID ) );

        // enforce rule update
        rule
            .withExpirationInDays( EXPIRATION_IN_DAYS )
            .withFilter( new LifecycleFilter( new LifecyclePrefixPredicate( ARTIFACTS_PREFIX + "/" ) ) )
            .withStatus( BucketLifecycleConfiguration.ENABLED );

        BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration().withRules( asList( rule ) );
        amazonS3.setBucketLifecycleConfiguration( BENCHMARKING_BUCKET_NAME, configuration );
    }

    private static String createBuildArtifactKey( String buildID, Path artifact )
    {
        return String.format( "%s/%s/%s", ARTIFACTS_PREFIX, buildID, artifact.toString() );
    }

}
