/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.stream.Stream;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;

import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.VERSION_41;

class SecurityGraphUpdatingCompatibility_41_IT extends SecurityGraphCompatibilityTestBase
{
    @Override
    void initEnterprise() throws Exception
    {
        initEnterprise( VERSION_41 );
    }

    @ParameterizedTest
    @MethodSource( "supportedPrivileges" )
    void shouldAllowCompatibleUpdatingCommandsOnOldGraph( PrivilegeCommand commands )
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
        HashSet<PrivilegeCommand> supported = new HashSet<>();
        supported.addAll( PRIVILEGES_ADDED_IN_40 );
        supported.addAll( PRIVILEGES_ADDED_IN_41D1 );
        supported.addAll( PRIVILEGES_ADDED_IN_41 );
        return supported.stream().map( Arguments::of );
    }
}
