/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import com.neo4j.causalclustering.protocol.ProtocolInstaller;

public interface ProtocolInstallers
{
    ProtocolInstaller<ProtocolInstaller.Orientation.Client> clientInstaller();

    ProtocolInstaller<ProtocolInstaller.Orientation.Server> serverInstaller();
}
