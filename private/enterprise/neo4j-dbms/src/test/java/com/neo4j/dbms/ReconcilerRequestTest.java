/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;


import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReconcilerRequestTest
{
    @Test
    void shouldCorrectlyIdentifySimpleRequests()
    {
        // given
        var simple = ReconcilerRequest.simple();
        var priority = ReconcilerRequest.priority( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) );
        var panicked = ReconcilerRequest.forPanickedDatabase( DatabaseIdFactory.from( "bar", UUID.randomUUID() ), new RuntimeException() );

        // when/then
        assertTrue( simple.isSimple() );
        assertFalse( priority.isSimple() );
        assertFalse( panicked.isSimple() );
    }

    @Test
    void shouldCorrectlyIdentifyPriorityRequests()
    {
        // given
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var priorityA = ReconcilerRequest.priority( Set.of( foo, bar ) );
        var priorityB = ReconcilerRequest.priority( bar );

        // when/then
        assertTrue( priorityA.isPriorityRequestForDatabase( "foo" ) );
        assertTrue( priorityA.isPriorityRequestForDatabase( "bar" ) );
        assertFalse( priorityB.isPriorityRequestForDatabase( "foo" ) );
    }
}
