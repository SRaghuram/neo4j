/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.Resource.GraphResource;
import com.neo4j.server.security.enterprise.auth.Resource.ProcedureResource;
import com.neo4j.server.security.enterprise.auth.Resource.SchemaResource;
import com.neo4j.server.security.enterprise.auth.Resource.SystemResource;
import com.neo4j.server.security.enterprise.auth.Resource.TokenResource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourcePrivilegeTest
{
    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            new ResourcePrivilege( privilegeType, Action.TRAVERSE, new GraphResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.TRAVERSE, new GraphResource(), RelTypeSegment.ALL );

            new ResourcePrivilege( privilegeType, Action.READ, new GraphResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.READ, new GraphResource(), RelTypeSegment.ALL );

            new ResourcePrivilege( privilegeType, Action.WRITE, new GraphResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.WRITE, new TokenResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.WRITE, new SchemaResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.WRITE, new SystemResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.WRITE, new GraphResource(), RelTypeSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.WRITE, new TokenResource(), RelTypeSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.WRITE, new SchemaResource(), RelTypeSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.WRITE, new SystemResource(), RelTypeSegment.ALL );

            new ResourcePrivilege( privilegeType, Action.EXECUTE, new ProcedureResource( "", "" ), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, Action.EXECUTE, new ProcedureResource( "", "" ), RelTypeSegment.ALL );
        }
    }

    @Test
    void shouldNotAcceptInvalidPrivileges()
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new TokenResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new SchemaResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new SystemResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new ProcedureResource( "", "" ), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new TokenResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new SchemaResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new SystemResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.TRAVERSE, new ProcedureResource( "", "" ), RelTypeSegment.ALL ) );

            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.READ, new TokenResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.READ, new SchemaResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.READ, new SystemResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.READ, new ProcedureResource( "", "" ), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, Action.READ, new TokenResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.READ, new SchemaResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.READ, new SystemResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.READ, new ProcedureResource( "", "" ), RelTypeSegment.ALL ) );

            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.WRITE, new ProcedureResource( "", "" ), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.WRITE, new ProcedureResource( "", "" ), RelTypeSegment.ALL ) );

            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new GraphResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new TokenResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new SchemaResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new SystemResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new GraphResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new TokenResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new SchemaResource(), RelTypeSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                    () -> new ResourcePrivilege( privilegeType, Action.EXECUTE, new SystemResource(), RelTypeSegment.ALL ) );
        }
    }
}
