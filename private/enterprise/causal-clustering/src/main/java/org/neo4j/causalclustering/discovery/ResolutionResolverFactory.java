/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.DiscoveryType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.internal.LogService;

public class ResolutionResolverFactory
{
    public static RemoteMembersResolver chooseResolver( Config config, LogService logService )
    {
        DiscoveryType discoveryType = config.get( CausalClusteringSettings.discovery_type );
        return discoveryType.getHostnameResolver( logService, config );
    }
}
