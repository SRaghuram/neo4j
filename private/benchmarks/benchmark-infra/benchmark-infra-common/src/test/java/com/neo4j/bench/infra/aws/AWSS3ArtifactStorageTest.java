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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AWSS3ArtifactStorageTest
{

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private S3Mock api;
    private Path s3Dir;
    private EndpointConfiguration endpointConfiguration;
    private AmazonS3 amazonS3;

    @Before
    public void setUp() throws Exception
    {
        s3Dir = temporaryFolder.newFolder( "s3" ).toPath();
        api = new S3Mock.Builder().withPort( 8001 ).withFileBackend( s3Dir.toString() ).build();
        api.start();

        endpointConfiguration = new EndpointConfiguration( "http://localhost:8001", "eu-north-1" );
        amazonS3 = AmazonS3Client.builder()
                                 .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) )
                                 .withEndpointConfiguration( endpointConfiguration )
                                 .withPathStyleAccessEnabled( true )
                                 .build();
        amazonS3.createBucket( AWSS3ArtifactStorage.BENCHMARKING_BUCKET_NAME );
    }

    @After
    public void tearDown()
    {
        api.shutdown();
    }

    @Test
    public void transferBuildArtifactWorkspace() throws Exception
    {
        // given
        Path directory = temporaryFolder.newFolder( "build" ).toPath();
        Files.createFile( directory.resolve( "artifact0.jar" ) );
        Files.createDirectories( directory.resolve( "artifact1" ) );
        Files.createFile( directory.resolve( "artifact1/artifact1.jar" ) );

        Workspace workspace = Workspace.create( directory )
                                       .withArtifacts(
                                               Paths.get( "artifact0.jar" ),
                                               Paths.get( "artifact1/artifact1.jar" )
                                       ).build();

        AWSS3ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( endpointConfiguration );
        //when
        URI artifactURI = URI.create( "s3://benchmarking.neo4j.com/artifacts/buildID" );
        artifactStorage.uploadBuildArtifacts( artifactURI, workspace );
        // then
        assertEquals(
                URI.create( "s3://benchmarking.neo4j.com/artifacts/buildID" ),
                artifactURI );

        assertTrue( Files.isRegularFile(
                s3Dir.resolve( "benchmarking.neo4j.com/artifacts/buildID" ).resolve( "artifact0.jar" ) ) );
        assertTrue( Files.isRegularFile(
                s3Dir.resolve( "benchmarking.neo4j.com/artifacts/buildID" ).resolve( "artifact1/artifact1.jar" ) ) );

        // when
        Path downloadDir = temporaryFolder.newFolder( "download" ).toPath();
        Workspace artifactsWorkspace = artifactStorage.downloadBuildArtifacts( downloadDir, artifactURI );
        // then
        assertTrue( isValid( workspace, downloadDir ) );
        assertThat( artifactsWorkspace.allArtifacts(),
                    containsInAnyOrder( downloadDir.resolve( "artifact0.jar" ),
                                        downloadDir.resolve( "artifact1/artifact1.jar" ) ) );
    }

    @Test
    public void downloadDataset() throws Exception
    {
        // given
        Path tempArchiveFile = createDatasetArchive();
        amazonS3.putObject( "benchmarking.neo4j.com", "datasets/macro/3.3.0-enterprise-datasets/dataset.tgz", tempArchiveFile.toFile() );
        ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( endpointConfiguration );

        // when
        String neo4jVersion = "3.3.0";
        String datasetName = "dataset";
        Dataset dataset = artifactStorage.downloadDataset( neo4jVersion, datasetName );
        Path tempFile = temporaryFolder.newFile( "dataset.tar.gz" ).toPath();
        dataset.copyInto( Files.newOutputStream( tempFile ) );

        // then
        assertEquals( CompressorStreamFactory.GZIP,
                      CompressorStreamFactory.detect( new BufferedInputStream( Files.newInputStream( tempFile ) ) ) );
    }

    @Test
    public void extractDataset() throws Exception
    {
        // given
        Path tempArchiveFile = createDatasetArchive();
        amazonS3.putObject( "benchmarking.neo4j.com", "datasets/macro/3.3.0-enterprise-datasets/dataset.tgz", tempArchiveFile.toFile() );
        ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( endpointConfiguration );

        // when
        String neo4jVersion = "3.3.0";
        String datasetName = "dataset";
        Dataset dataset = artifactStorage.downloadDataset( neo4jVersion, datasetName );
        Path tempDir = temporaryFolder.newFolder( "dataset" ).toPath();
        dataset.extractInto( tempDir );

        // then
        assertTrue( Files.isRegularFile( tempDir.resolve( "data.txt" ) ) );
    }

    // lots of ceremony, but I want to be sure we are downloading the right thing
    private Path createDatasetArchive() throws IOException, CompressorException, ArchiveException
    {
        Path tempDataFile = temporaryFolder.newFile( "datafile.txt" ).toPath();
        Files.write( tempDataFile, Arrays.asList( "data" ) );

        Path tempArchiveFile = temporaryFolder.newFile().toPath();

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

    public boolean isValid( Workspace workspace, Path anotherBaseDir )
    {
        return workspace.allArtifacts().stream()
                        .map( workspace.baseDir()::relativize )
                        .map( anotherBaseDir::resolve )
                        .allMatch( Files::isRegularFile );
    }
}
