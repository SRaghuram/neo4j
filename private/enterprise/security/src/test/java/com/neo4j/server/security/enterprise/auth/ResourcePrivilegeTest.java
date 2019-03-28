/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourcePrivilegeTest
{
    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        new ResourcePrivilege( "read", "graph" );

        new ResourcePrivilege( "write", "graph" );
        new ResourcePrivilege( "write", "token" );
        new ResourcePrivilege( "write", "schema" );
        new ResourcePrivilege( "write", "system" );

        new ResourcePrivilege( "execute", "procedure" );
    }

    @Test
    void shouldNotAcceptInvalidPrivileges()
    {
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "read", "token" ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "read", "schema" ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "read", "system" ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "read", "procedure" ) );

        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "write", "procedure" ) );

        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "execute", "graph" ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "execute", "token" ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "execute", "schema" ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( "execute", "system" ) );
    }
}
