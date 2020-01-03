/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.RoleInfo;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;

public class ReadReplicaRoleProcedure extends RoleProcedure
{
    public ReadReplicaRoleProcedure( DatabaseManager<?> databaseManager )
    {
        super( databaseManager );
    }

    @Override
    RoleInfo role( NamedDatabaseId namedDatabaseId )
    {
        return databaseManager.getDatabaseContext( namedDatabaseId )
                .map( DatabaseContext::database )
                .filter( db -> db.getDatabaseAvailabilityGuard().isAvailable() )
                .map( ctx -> RoleInfo.READ_REPLICA )
                .orElse( RoleInfo.UNKNOWN );
    }
}
