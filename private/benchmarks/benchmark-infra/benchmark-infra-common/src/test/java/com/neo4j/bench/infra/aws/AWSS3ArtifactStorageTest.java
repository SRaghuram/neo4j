/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.Workspace;
import io.findify.s3mock.S3Mock;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AWSS3ArtifactStorageTest
{
    private S3Mock api;
    private Path s3Dir;
    private EndpointConfiguration endpointConfiguration;
    private AmazonS3 amazonS3;

    @BeforeEach
    void setUp() throws Exception
    {
        s3Dir = Files.createTempDirectory( "s3" );
        api = new S3Mock.Builder().withPort( 8001 ).withFileBackend( s3Dir.toString() ).build();
        api.start();

        endpointConfiguration = new EndpointConfiguration( "http://localhost:8001", "eu-north-1" );
        amazonS3 = AmazonS3Client.builder()
                .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) )
                .withEndpointConfiguration( endpointConfiguration )
                .withPathStyleAccessEnabled( true )
                .build();
        amazonS3.createBucket(  AWSS3ArtifactStorage.BENCHMARKING_BUCKET_NAME );

    }

    @AfterEach
    void tearDown()
    {
        api.shutdown();
    }

    @Test
    void transferBuildArtifactWorkspace() throws Exception
    {
        // given
        Path directory = Files.createTempDirectory( "build" );
        Path artifact0 = Files.createFile( directory.resolve( "artifact0.jar" ) );
        Files.createDirectories( directory.resolve( "artifact1" ) );
        Files.createFile( directory.resolve( "artifact1/artifact1.jar" ) );

        Workspace workspace = Workspace.create( directory )
                .withArtifacts(
                    Paths.get( "artifact0.jar" ),
                    Paths.get( "artifact1/artifact1.jar" )
                 ).build();

        AWSS3ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( endpointConfiguration );
        //when
        URI uri = URI.create( "s3://benchmarking.neo4j.com/artifacts/buildID" );
        artifactStorage.verifyBuildArtifactsExpirationRule( uri );
        URI artifactURI = artifactStorage.uploadBuildArtifacts( uri, workspace );
        // then
        assertEquals(
                URI.create( "s3://benchmarking.neo4j.com/artifacts/buildID" ),
                artifactURI );

        System.out.println( s3Dir.resolve( "benchmarking.neo4j.com/artifacts/buildID" ).resolve( artifact0.getFileName() ).toFile().toString() );

        assertTrue( Files
                .isRegularFile(
                        s3Dir
                                .resolve( "benchmarking.neo4j.com/artifacts/buildID" )
                                .resolve( artifact0.getFileName() ) ) );
        // when
        Path downloadDir = Files.createTempDirectory( "download" );
        artifactStorage.downloadBuildArtifacts( downloadDir, uri );
        // then
        assertTrue(workspace.isValid(downloadDir));

    }

    @Test
    void dowloadDataset() throws Exception
    {
        // given
        Path tempArchiveFile = createDatasetArchive();
        amazonS3.putObject( "benchmarking.neo4j.com","datasets/macro/3.3.0-enterprise-datasets/dataset.tgz", tempArchiveFile.toFile() );
        ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( endpointConfiguration );

        // when
        String neo4jVersion = "3.3.0";
        String datasetName = "dataset";
        Dataset dataset = artifactStorage.downloadDataset( neo4jVersion, datasetName );
        Path tempFile = Files.createTempFile( "dataset-", ".tar.gz" );
        dataset.copyInto( Files.newOutputStream( tempFile ) );

        // then
        assertEquals( CompressorStreamFactory.GZIP,
                CompressorStreamFactory.detect( new BufferedInputStream( Files.newInputStream( tempFile ) ) ) );
    }

    @Test
    void extractDataset() throws Exception
    {
        // given
        Path tempArchiveFile = createDatasetArchive();
        amazonS3.putObject( "benchmarking.neo4j.com","datasets/macro/3.3.0-enterprise-datasets/dataset.tgz", tempArchiveFile.toFile() );
        ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( endpointConfiguration );

        // when
        String neo4jVersion = "3.3.0";
        String datasetName = "dataset";
        Dataset dataset = artifactStorage.downloadDataset( neo4jVersion, datasetName );
        Path tempDir = Files.createTempDirectory( "dataset" );
        dataset.extractInto( tempDir );

        // then
        assertTrue( Files.isRegularFile( tempDir.resolve( "data.txt" ) ) );
    }

    // lots of ceremony, but I want to be sure we are downloading the right thing
    private static Path createDatasetArchive() throws IOException, CompressorException, ArchiveException
    {
        Path tempDataFile = Files.createTempFile( "datafile-", ".txt" );
        Files.write( tempDataFile, Arrays.asList( "data" ) );

        Path tempArchiveFile = Files.createTempFile( "dataset-", ".tar.gz" );

        try ( CompressorOutputStream compressorOutput = new CompressorStreamFactory()
                .createCompressorOutputStream( CompressorStreamFactory.GZIP, Files.newOutputStream( tempArchiveFile ) );
                ArchiveOutputStream archiveOutput =
                new ArchiveStreamFactory().createArchiveOutputStream( ArchiveStreamFactory.TAR, compressorOutput ) )
        {
            ArchiveEntry archiveEntry = archiveOutput.createArchiveEntry( tempDataFile.toFile(), "data.txt" );
            archiveOutput.putArchiveEntry( archiveEntry );
            IOUtils.copy( Files.newInputStream( tempDataFile ), archiveOutput );
            archiveOutput.closeArchiveEntry();
        }
        return tempArchiveFile;
    }
}
