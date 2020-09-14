/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ImpermanentEnterpriseDbmsExtension
class PrivilegesAsCommandsIT
{
    @Inject
    DatabaseManagementService dbms;

    @Inject
    GraphDatabaseAPI gdb;

    @Test
    void shouldReplicateInitialState()
    {
        GraphDatabaseService system = dbms.database( SYSTEM_DATABASE_NAME );
        final EnterpriseSecurityGraphComponent enterpriseSecurityGraphComponent =
                gdb.getDependencyResolver().resolveDependency( EnterpriseSecurityGraphComponent.class );
        try ( Transaction tx = system.beginTx() )
        {
            enterpriseSecurityGraphComponent.getPrivilegesAsCommands( tx, "neo4j", true ).forEach( System.out::println );
        }
    }
}
