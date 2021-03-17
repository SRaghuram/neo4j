/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.model.model.Neo4jConfig;

import java.net.URI;
import java.nio.file.Path;

public interface Agent
{
    int DEFAULT_PORT = 9080;
    String DEFAULT_WORK_DIRECTORY = "/tmp/agentWork";

    boolean ping();

    AgentState state();

    boolean stopAgent();

    boolean prepare( URI artifactBaseUri,
                     String productArchive,
                     URI dataSetBaseUri,
                     String datasetName,
                     Version neo4jVersion );

    URI startDatabase( String placeHolderForProfilerInfos,
                       boolean copyStore,
                       Neo4jConfig neo4jConfig );

    void stopDatabase();

    void downloadResults( Path downloadResultsInto );
}
