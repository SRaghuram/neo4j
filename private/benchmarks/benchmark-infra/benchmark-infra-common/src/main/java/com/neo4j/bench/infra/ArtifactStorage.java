/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

public interface ArtifactStorage
{

    URI uploadBuildArtifacts( String string, Workspace workspace ) throws URISyntaxException, AmazonServiceException, SdkClientException, IOException;

    void downloadBuildArtifacts( Path baseDir, String buildID ) throws IOException;

    Dataset downloadDataset( String neo4jVersion, String dataset );
}
