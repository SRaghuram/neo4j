/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ResourcePrivilegeTest
{
    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        new ResourcePrivilege( Action.READ, new Resource.GraphResource(), LabelSegment.ALL );

        new ResourcePrivilege( Action.WRITE, new Resource.GraphResource(), LabelSegment.ALL );
        new ResourcePrivilege( Action.WRITE, new Resource.TokenResource(), LabelSegment.ALL );
        new ResourcePrivilege( Action.WRITE, new Resource.SchemaResource(), LabelSegment.ALL );
        new ResourcePrivilege( Action.WRITE, new Resource.SystemResource(), LabelSegment.ALL );

        new ResourcePrivilege( Action.EXECUTE, new Resource.ProcedureResource( "", "" ), LabelSegment.ALL );
    }

    @Test
    void shouldNotAcceptInvalidPrivileges()
    {
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.READ, new Resource.TokenResource(), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.READ, new Resource.SchemaResource(), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.READ, new Resource.SystemResource(), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class,
                () -> new ResourcePrivilege( Action.READ, new Resource.ProcedureResource( "", "" ), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class,
                () -> new ResourcePrivilege( Action.WRITE, new Resource.ProcedureResource( "", "" ), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, new Resource.GraphResource(), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, new Resource.TokenResource(), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, new Resource.SchemaResource(), LabelSegment.ALL ) );
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( Action.EXECUTE, new Resource.SystemResource(), LabelSegment.ALL ) );
    }
}
