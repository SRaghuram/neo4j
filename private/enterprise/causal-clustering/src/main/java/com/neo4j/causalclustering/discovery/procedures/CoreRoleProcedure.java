/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.discovery.RoleInfo;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;

public class CoreRoleProcedure extends RoleProcedure
{
    public CoreRoleProcedure( DatabaseManager<?> databaseManager )
    {
        super( databaseManager );
    }

    @Override
    RoleInfo role( DatabaseContext databaseContext ) throws ProcedureException
    {
        var raftMachine = databaseContext.dependencies().resolveDependency( RaftMachine.class );
        if ( raftMachine == null )
        {
            throw new ProcedureException( Status.General.UnknownError, "Unable to resolve role for database. This may be because the database is stopping." );
        }
        return raftMachine.isLeader() ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
    }
}
