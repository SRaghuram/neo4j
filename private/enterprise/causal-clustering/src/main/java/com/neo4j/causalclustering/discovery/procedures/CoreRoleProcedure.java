/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.core.IdentityModule;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.discovery.RoleInfo;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;

public class CoreRoleProcedure extends RoleProcedure
{
    public CoreRoleProcedure( DatabaseManager<?> databaseManager )
    {
        super( databaseManager );
    }

    @Override
    RoleInfo role( DatabaseContext databaseContext )
    {
        return databaseContext.dependencies().resolveDependency( RaftMachine.class ).isLeader() ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
    }
}
