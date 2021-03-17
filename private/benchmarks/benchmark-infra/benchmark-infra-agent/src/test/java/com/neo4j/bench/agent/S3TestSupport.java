/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.Compressor;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.test.ports.PortAuthority;

import static java.lang.String.format;

public class S3TestSupport implements AutoCloseable
{
    public static final String S3_BUCKET = "benchmarking.neo4j.com";
    public static final URI ARTIFACT_BASE_URI = URI.create( format( "s3://%s/artifacts", S3_BUCKET ) );
    public static final URI DATASET_BASE_URI = URI.create( format( "s3://%s/datasets", S3_BUCKET ) );
    private static String awsEndpoint;

    private final S3Mock s3api;
    private final ArtifactStorage artifactStorage;

    private String datasetName;
    private String packageName;
    private String packageArchive;

    private Path s3RootFolder;

    public S3TestSupport( Path s3RootFolder, String datasetName ) throws UnknownHostException
    {
        this.s3RootFolder = s3RootFolder;
        this.s3api = createS3( s3RootFolder );
        this.artifactStorage = createArtifactStorage( s3api );
        this.datasetName = datasetName;
    }

    @Override
    public void close()
    {
        artifactStorage.close();
        s3api.shutdown();
    }

    public void setupPackage() throws IOException
    {
        Path originalDir = Paths.get( System.getenv( "NEO4J_DIR" ) );
        this.packageName = originalDir.getFileName().toString();
        this.packageArchive = packageName + ".tar.gz";
        Path packageDir = s3RootFolder.resolve( "tempPackage" );
        FileUtils.copyDirectory( originalDir.toFile(), packageDir.resolve( packageName ).toFile() );
        uploadDirectoryToS3AndRemove( s3RootFolder, packageDir, "/artifacts", packageArchive );
    }

    public void setupDataset( Version version ) throws IOException
    {
        InputStream datasetOnClasspath = this.getClass().getClassLoader().getResourceAsStream( datasetName + ".tgz" );
        String targetFile = format( "%s/datasets/%s-enterprise-datasets/%s.tgz", S3_BUCKET, version.minorVersion(), datasetName );
        Path datasetFile = s3RootFolder.resolve( targetFile );
        FileUtils.forceMkdir( datasetFile.getParent().toFile() );
        Files.createFile( datasetFile );
        IOUtils.copy( datasetOnClasspath, new FileOutputStream( datasetFile.toFile() ) );
    }

    public String packageArchive()
    {
        return packageArchive;
    }

    public String packageName()
    {
        return packageName;
    }

    public String datasetName()
    {
        return datasetName;
    }

    public ArtifactStorage artifactStorage()
    {
        return artifactStorage;
    }

    public String awsEndpoint()
    {
        return awsEndpoint;
    }

    private static S3Mock createS3( Path s3Path )
    {
        return new S3Mock.Builder()
                .withPort( PortAuthority.allocatePort() )
                .withFileBackend( s3Path.toString() )
                .build();
    }

    private static ArtifactStorage createArtifactStorage( S3Mock s3api ) throws UnknownHostException
    {
        InetSocketAddress awsEndpointLocalAddress = s3api.start().localAddress();
        awsEndpoint = format( "http://%s:%d", InetAddress.getLocalHost().getHostName(), awsEndpointLocalAddress.getPort() );
        AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                                                 .withPathStyleAccessEnabled( true )
                                                 .withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration( awsEndpoint, "eu-north-1" ) )
                                                 .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) )
                                                 .build();
        s3client.createBucket( S3_BUCKET );
        s3client.shutdown();
        return AWSS3ArtifactStorage.create( new AwsClientBuilder.EndpointConfiguration( awsEndpoint, "eu-north-1" ) );
    }

    private static void uploadDirectoryToS3AndRemove( Path s3Root, Path sourceDir, String targetDir, String targetName ) throws IOException
    {
        Path s3Directory = s3Root.resolve( S3_BUCKET + targetDir );
        FileUtils.forceMkdir( s3Directory.toFile() );
        Compressor.compress( s3Directory.resolve( targetName ), sourceDir );
        FileUtils.deleteDirectory( sourceDir.toFile() );
    }
}
