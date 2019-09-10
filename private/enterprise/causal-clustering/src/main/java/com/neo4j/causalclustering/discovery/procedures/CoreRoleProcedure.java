/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.core.IdentityModule;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;

public class CoreRoleProcedure extends RoleProcedure
{
    private final IdentityModule identityModule;
    private final TopologyService topologyService;

    public CoreRoleProcedure( IdentityModule identityModule, TopologyService topologyService, DatabaseManager<?> databaseManager )
    {
        super( databaseManager );
        this.identityModule = identityModule;
        this.topologyService = topologyService;
    }

    @Override
    RoleInfo role( NamedDatabaseId namedDatabaseId )
    {
        var myId = identityModule.myself();
        return topologyService.lookupRole( namedDatabaseId, myId );
    }
}
