/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import java.time.Clock;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholdPolicy;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.LogProvider;

@ServiceProvider
public class VolumetricCheckPointPolicy implements CheckPointThresholdPolicy
{
    @Override
    public String getName()
    {
        return "volumetric";
    }

    @Override
    public CheckPointThreshold createThreshold(
            Config config, Clock clock, LogPruning logPruning, LogProvider logProvider )
    {
        return new VolumetricCheckPointThreshold( logPruning );
    }
}
