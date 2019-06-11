/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.core.IdentityModule;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

public class CoreRoleProcedure extends RoleProcedure
{
    private final IdentityModule identityModule;
    private final TopologyService topologyService;

    public CoreRoleProcedure( IdentityModule identityModule, TopologyService topologyService, DatabaseIdRepository databaseIdRepository )
    {
        super( databaseIdRepository );
        this.identityModule = identityModule;
        this.topologyService = topologyService;
    }

    @Override
    RoleInfo role( DatabaseId databaseId )
    {
        var myId = identityModule.myself();
        return topologyService.coreRole( databaseId, myId );
    }
}
