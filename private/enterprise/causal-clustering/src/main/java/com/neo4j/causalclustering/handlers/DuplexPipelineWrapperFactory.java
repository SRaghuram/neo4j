/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.handlers;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;

public interface DuplexPipelineWrapperFactory
{
    PipelineWrapper forServer( Config config, Dependencies dependencies, LogProvider logProvider, Setting<String> policyName );

    PipelineWrapper forClient( Config config, Dependencies dependencies, LogProvider logProvider, Setting<String> policyName );
}
