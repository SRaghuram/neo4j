/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Optional;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.LogProvider;

public class TopologyServiceMultiRetryStrategy extends MultiRetryStrategy<MemberId,Optional<AdvertisedSocketAddress>> implements TopologyServiceRetryStrategy
{
    public TopologyServiceMultiRetryStrategy( long delayInMillis, long retries, LogProvider logProvider )
    {
        super( delayInMillis, retries, logProvider, TopologyServiceMultiRetryStrategy::sleep );
    }

    private static void sleep( long durationInMillis )
    {
        try
        {
            Thread.sleep( durationInMillis );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
