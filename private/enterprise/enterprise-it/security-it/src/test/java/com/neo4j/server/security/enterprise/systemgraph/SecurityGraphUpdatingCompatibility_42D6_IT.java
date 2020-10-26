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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_42D6;

class SecurityGraphUpdatingCompatibility_42D6_IT extends SecurityGraphCompatibilityTestBase
{
    @Override
    void initEnterprise() throws Exception
    {
        initEnterprise( VERSION_42D6 );
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

    @ParameterizedTest
    @MethodSource( "unsupportedPrivileges" )
    void shouldFailOnNewCommandsOnOldGraph( PrivilegeCommand commands )
    {
        for ( String query : commands.asCypher() )
        {
            try ( Transaction tx = system.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
            {
                var exception = assertThrows( UnsupportedOperationException.class, () -> tx.execute( query ), query );
                assertThat( exception.getMessage() )
                        .contains( "This operation is not supported while running in compatibility mode with version " + VERSION_42D6 );
            }
        }
    }

    private static Stream<Arguments> unsupportedPrivileges()
    {
        return ALL_PRIVILEGES.stream()
                             .filter( p -> !PRIVILEGES_ADDED_IN_40.contains( p ) )
                             .filter( p -> !PRIVILEGES_ADDED_IN_41D1.contains( p ) )
                             .filter( p -> !PRIVILEGES_ADDED_IN_41.contains( p ) )
                             .filter( p -> !PRIVILEGES_ADDED_IN_42D4.contains( p ) )
                             .filter( p -> !PRIVILEGES_ADDED_IN_42D6.contains( p ) )
                             .map( Arguments::of );
    }

    private static Stream<Arguments> supportedPrivileges()
    {
        HashSet<PrivilegeCommand> supported = new HashSet<>();
        supported.addAll( PRIVILEGES_ADDED_IN_40 );
        supported.addAll( PRIVILEGES_ADDED_IN_41D1 );
        supported.addAll( PRIVILEGES_ADDED_IN_41 );
        supported.addAll( PRIVILEGES_ADDED_IN_42D4 );
        supported.addAll( PRIVILEGES_ADDED_IN_42D6 );
        return supported.stream().map( Arguments::of );
    }
}