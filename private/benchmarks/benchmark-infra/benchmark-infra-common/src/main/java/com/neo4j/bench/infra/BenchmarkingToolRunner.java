/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

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
    void runTool( JobParams<P> jobParams,
                  ArtifactStorage artifactStorage,
                  Path workspacePath,
                  Workspace artifactsWorkspace,
                  ResultStoreCredentials resultsStoreCredentials,
                  URI artifactBaseUri )
            throws Exception;
}
