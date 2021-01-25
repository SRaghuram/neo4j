/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import java.net.URI;
import java.nio.file.Path;

/**
 * A place where build artifacts are stored. There are two types of artifacts:
 * <ul>
 * <li>build artifacts, including benchmarks jars and run scripts</li>
 * <li>data sets</li>
 * </ul>
 */
public interface ArtifactStorage
{

    /**
     * Uploads build artifacts into artifacts storage.
     *
     * @param artifactBaseURI the bucket to upload the files to, should exist
     * @param workspace description of build artifacts which are included in workspace
     * @throws ArtifactStoreException
     */
    void uploadBuildArtifacts( URI artifactBaseURI, Workspace workspace ) throws ArtifactStoreException;

    /**
     * Downloads build artifacts from artifact storage.
     *
     * @param baseDir base directory, where artifacts will be downloaded
     * @param artifactBaseURI the bucket to download the files from, should exist
     * @param goalWorkspace the workspace that we want to download
     * @throws ArtifactStoreException
     * @return workspace with downloaded artifacts
     */
    Workspace downloadBuildArtifacts( Path baseDir, URI artifactBaseURI, Workspace goalWorkspace ) throws ArtifactStoreException;

    /**
     * Downloads Workspace Structure.
     *
     *
     * @param jobParameters file name of the job parameters json file
     * @param baseDir base directory, where artifacts will be downloaded
     * @param artifactBaseURI the bucket to download the files from, should exist
     * @throws ArtifactStoreException
     * @return workspace with downloaded artifacts
     */
    Path downloadParameterFile( String jobParameters, Path baseDir, URI artifactBaseURI ) throws ArtifactStoreException;

    /**
     * Downloads data set for specific Neo4j version
     * @param dataSetBaseUri base uri to bucket + prefix where to look for dataset
     * @param neo4jVersion Neo4j version
     * @param dataset a name of dataset
     * @return downloaded data set
     */
    Dataset downloadDataset( URI dataSetBaseUri,String neo4jVersion, String dataset );
}
