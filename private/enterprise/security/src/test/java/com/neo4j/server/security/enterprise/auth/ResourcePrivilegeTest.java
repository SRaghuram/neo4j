/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.Resource.AllPropertiesResource;
import com.neo4j.server.security.enterprise.auth.Resource.DatabaseResource;
import com.neo4j.server.security.enterprise.auth.Resource.GraphResource;
import com.neo4j.server.security.enterprise.auth.Resource.ProcedureResource;
import com.neo4j.server.security.enterprise.auth.Resource.PropertyResource;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.*;

class ResourcePrivilegeTest
{
    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            for ( PrivilegeAction action : PrivilegeAction.values() )
            {
                if ( ACCESS.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( TRAVERSE.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new GraphResource() );
                }
                else if ( READ.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new GraphResource() );
                    assertOk( privilegeType, action, new AllPropertiesResource() );
                    assertOk( privilegeType, action, new PropertyResource( "foo" ) );
                }
                else if ( WRITE.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new GraphResource() );
                    assertOk( privilegeType, action, new AllPropertiesResource() );
                    assertOk( privilegeType, action, new PropertyResource( "foo" ) );
                }
                else if ( ADMIN.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( SCHEMA.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( TOKEN.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( EXECUTE.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new ProcedureResource( "", "" ) );
                }
                else
                {
                    fail( "Unhandled PrivilegeAction: " + action );
                }
            }
        }
    }

    @Test
    void shouldNotAcceptInvalidPrivileges()
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            for ( PrivilegeAction action : PrivilegeAction.values() )
            {
                if ( ACCESS.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new ProcedureResource( "", "" ) );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                }
                else if ( TRAVERSE.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new ProcedureResource( "", "" ) );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new DatabaseResource() );
                }
                else if ( READ.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new ProcedureResource( "", "" ) );
                    assertFail( privilegeType, action, new DatabaseResource() );
                }
                else if ( WRITE.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new ProcedureResource( "", "" ) );
                    assertFail( privilegeType, action, new DatabaseResource() );
                }
                else if ( ADMIN.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new ProcedureResource( "", "" ) );
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                }
                else if ( SCHEMA.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new ProcedureResource( "", "" ) );
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                }
                else if ( TOKEN.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new ProcedureResource( "", "" ) );
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                }
                else if ( EXECUTE.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new DatabaseResource() );
                }
                else
                {
                    fail( "Unhandled PrivilegeAction: " + action );
                }
            }
        }
    }

    private void assertFail( ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action, Resource resource )
    {
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, action, resource, TEST_SEGMENT ) );
    }

    private void assertOk( ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action, Resource resource ) throws InvalidArgumentsException
    {
        new ResourcePrivilege( privilegeType, action, resource, TEST_SEGMENT );
    }

    private static Segment TEST_SEGMENT = new Segment()
    {
    };
}
