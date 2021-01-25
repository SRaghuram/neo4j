/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.RoleInfo;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadReplicaRoleProcedureTest extends RoleProcedureTest
{
    ReadReplicaRoleProcedureTest()
    {
        super( ReadReplicaRoleProcedure::new );
    }

    @Test
    void shouldReturnReadReplicaRole() throws ProcedureException
    {
        mockAvailableExitingDatabase();
        assertEquals( RoleInfo.READ_REPLICA, runProcedure() );
    }
}
