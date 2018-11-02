/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.server.enterprise.EnterpriseServerSettings;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIs;

public class EnterpriseDiscoverableURIs
{
    public static DiscoverableURIs enterpriseDiscoverableURIs( Config config, ConnectorPortRegister ports )
    {
        DiscoverableURIs uris = communityDiscoverableURIs( config, ports );
        if ( config.get( EnterpriseEditionSettings.mode ) == EnterpriseEditionSettings.Mode.CORE )
        {
            DiscoverableURIs
                    .discoverableBoltUri( "bolt+routing", config,
                            EnterpriseServerSettings.bolt_routing_discoverable_address, ports )
                    .ifPresent( uri -> uris.addAbsolute( "bolt_routing", uri ) );
        }
        return uris;
    }
}
