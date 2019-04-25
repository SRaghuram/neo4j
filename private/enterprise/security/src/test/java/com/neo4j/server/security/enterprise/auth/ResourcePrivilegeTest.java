/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Resource;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class ResourcePrivilegeTest
{
    @Test
    void shouldHaveUpdatedTestsForAllPrivileges()
    {
        for ( Action action : Action.values() )
        {
            switch ( action )
            {
            case READ:
            case WRITE:
            case EXECUTE:
                break;
            default:
                fail( String.format( "new action (%s) added but tests not updated." +
                        " Make sure that StandardCommercialLoginContext.StandardAccessMode.Builder.addPrivileges is updated as well", action.toString() ) );
            }
        }
        for ( Resource resource : Resource.values() )
        {
            switch ( resource )
            {
            case GRAPH:
            case TOKEN:
            case SCHEMA:
            case SYSTEM:
            case PROCEDURE:
                break;
            default:
                fail( String.format( "new resource (%s) added but tests not updated." +
                        " Make sure that StandardCommercialLoginContext.StandardAccessMode.Builder.addPrivileges is updated as well", resource.toString() ) );
            }
        }
    }

    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        new ResourcePrivilege( Action.READ, Resource.GRAPH );

        new ResourcePrivilege( Action.WRITE, Resource.GRAPH );
        new ResourcePrivilege( Action.WRITE, Resource.TOKEN );
        new ResourcePrivilege( Action.WRITE, Resource.SCHEMA );
        new ResourcePrivilege( Action.WRITE, Resource.SYSTEM );

        new ResourcePrivilege( Action.EXECUTE, Resource.PROCEDURE );
    }

    @Test
    void shouldNotAcceptInvalidPrivileges()
    {
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.READ, Resource.TOKEN ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.READ, Resource.SCHEMA ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.READ, Resource.SYSTEM ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.READ, Resource.PROCEDURE ) );

        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.WRITE, Resource.PROCEDURE ) );

        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, Resource.GRAPH ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, Resource.TOKEN ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, Resource.SCHEMA ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, Resource.SYSTEM ) );
    }
}
