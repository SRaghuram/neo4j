/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;

import java.net.URI;
import java.nio.file.Path;

/**
 * Runs benchmarking tool in batch infrastructure worker.
 */
public interface BenchmarkingToolRunner<P>
{

    /**
     * Run benchmarking tool.
     */
    void runTool( P toolParams,
                  AWSS3ArtifactStorage artifactStorage,
                  Path workspacePath,
                  Workspace artifactsWorkspace,
                  InfraParams infraParams,
                  String resultsStorePassword,
                  String batchJobId,
                  URI artifactBaseUri )
            throws Exception;
}
