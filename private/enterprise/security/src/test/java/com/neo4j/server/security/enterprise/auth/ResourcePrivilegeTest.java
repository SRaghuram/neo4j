/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.Resource.DatabaseResource;
import com.neo4j.server.security.enterprise.auth.Resource.GraphResource;
import com.neo4j.server.security.enterprise.auth.Resource.ProcedureResource;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourcePrivilegeTest
{
    // TODO expand these tests
    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            new ResourcePrivilege( privilegeType, PrivilegeAction.ACCESS, new DatabaseResource(), DatabaseSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.START_DATABASE, new DatabaseResource(), DatabaseSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.STOP_DATABASE, new DatabaseResource(), DatabaseSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.ADMIN, new DatabaseResource(), DatabaseSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.SCHEMA, new DatabaseResource(), DatabaseSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.TOKEN, new DatabaseResource(), DatabaseSegment.ALL );

            new ResourcePrivilege( privilegeType, PrivilegeAction.TRAVERSE, new GraphResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.TRAVERSE, new GraphResource(), RelTypeSegment.ALL );

            new ResourcePrivilege( privilegeType, PrivilegeAction.READ, new GraphResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.READ, new GraphResource(), RelTypeSegment.ALL );

            new ResourcePrivilege( privilegeType, PrivilegeAction.WRITE, new GraphResource(), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.WRITE, new GraphResource(), RelTypeSegment.ALL );

            new ResourcePrivilege( privilegeType, PrivilegeAction.EXECUTE, new ProcedureResource( "", "" ), LabelSegment.ALL );
            new ResourcePrivilege( privilegeType, PrivilegeAction.EXECUTE, new ProcedureResource( "", "" ), RelTypeSegment.ALL );
        }
    }

    @Test
    void shouldNotAcceptInvalidPrivileges()
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.TRAVERSE, new ProcedureResource( "", "" ), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.TRAVERSE, new ProcedureResource( "", "" ), RelTypeSegment.ALL ) );

            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.READ, new ProcedureResource( "", "" ), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.READ, new ProcedureResource( "", "" ), RelTypeSegment.ALL ) );

            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.WRITE, new ProcedureResource( "", "" ), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.WRITE, new ProcedureResource( "", "" ), RelTypeSegment.ALL ) );

            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.EXECUTE, new GraphResource(), LabelSegment.ALL ) );
            assertThrows( InvalidArgumentsException.class,
                          () -> new ResourcePrivilege( privilegeType, PrivilegeAction.EXECUTE, new GraphResource(), RelTypeSegment.ALL ) );
        }
    }
}
