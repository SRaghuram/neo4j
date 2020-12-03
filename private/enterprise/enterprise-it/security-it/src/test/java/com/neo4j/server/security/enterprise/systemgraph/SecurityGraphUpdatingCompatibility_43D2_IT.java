/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;

import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_43D2;

class SecurityGraphUpdatingCompatibility_43D2_IT extends SecurityGraphCompatibilityTestBase
{
    @Override
    void initEnterprise() throws Exception
    {
        initEnterprise( VERSION_43D2);
    }

    @ParameterizedTest
    @MethodSource( "supportedPrivileges" )
    void shouldAllowCompatibleUpdatingCommands( PrivilegeCommand commands )
    {
        try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            for ( String command : commands.asCypher() )
            {
                tx.execute( command );
            }
            tx.commit();
        }
    }

    private static Stream<Arguments> supportedPrivileges()
    {
        return ALL_PRIVILEGES.stream().map( Arguments::of );
    }
}
