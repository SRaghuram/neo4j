/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery.procedures;

import org.neo4j.causalclustering.discovery.RoleInfo;

public class ReadReplicaRoleProcedure extends RoleProcedure
{
    public ReadReplicaRoleProcedure()
    {
        super();
    }

    @Override
    RoleInfo role()
    {
        return RoleInfo.READ_REPLICA;
    }
}
