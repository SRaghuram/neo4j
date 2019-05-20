/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
     * @param buildID unique build id
     * @param workspace description of build artifacts which are included in workspace
     * @return URI to uploaded location
     * @throws ArtifactStoreException
     */
    URI uploadBuildArtifacts( String buildID, Workspace workspace ) throws ArtifactStoreException;

    /**
     * Downloads build artifacts from artifact storage.
     *
     * @param baseDir base directory, where artifacts will be downloaded
     * @param buildID unique build id
     * @throws ArtifactStoreException
     */
    void downloadBuildArtifacts( Path baseDir, String buildID ) throws ArtifactStoreException;

    /**
     * Downloads data set for specific Neo4j version
     * @param neo4jVersion Neo4j version
     * @param dataset a name of dataset
     * @return downloaded data set
     */
    Dataset downloadDataset( String neo4jVersion, String dataset );
}
