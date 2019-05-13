/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.RoleInfo;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

public class ReadReplicaRoleProcedure extends RoleProcedure
{
    public ReadReplicaRoleProcedure( DatabaseIdRepository databaseIdRepository )
    {
        super( databaseIdRepository );
    }

    @Override
    RoleInfo role( DatabaseId databaseId )
    {
        return RoleInfo.READ_REPLICA;
    }
}
