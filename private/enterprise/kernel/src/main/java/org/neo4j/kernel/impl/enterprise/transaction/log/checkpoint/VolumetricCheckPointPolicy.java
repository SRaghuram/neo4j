/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholdPolicy;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.SystemNanoClock;

@Service.Implementation( CheckPointThresholdPolicy.class )
public class VolumetricCheckPointPolicy extends CheckPointThresholdPolicy
{
    public VolumetricCheckPointPolicy()
    {
        super( "volumetric" );
    }

    @Override
    public CheckPointThreshold createThreshold(
            Config config, SystemNanoClock clock, LogPruning logPruning, LogProvider logProvider )
    {
        return new VolumetricCheckPointThreshold( logPruning );
    }
}
