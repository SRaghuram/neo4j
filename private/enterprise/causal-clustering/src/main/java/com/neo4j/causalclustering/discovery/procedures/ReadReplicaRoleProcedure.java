/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.RoleInfo;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;

public class ReadReplicaRoleProcedure extends RoleProcedure
{
    public ReadReplicaRoleProcedure( DatabaseManager<?> databaseManager )
    {
        super( databaseManager );
    }

    @Override
    RoleInfo role( DatabaseContext namedDatabaseId )
    {
        return RoleInfo.READ_REPLICA;
    }
}
