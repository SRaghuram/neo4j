/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import org.neo4j.lock.ResourceTypes;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;

class LockUnitTest
{
    @Test
    void exclusiveLocksAppearFirst()
    {
        LockUnit unit1 = new LockUnit( ResourceTypes.NODE, EXCLUSIVE, 1, 1 );
        LockUnit unit2 = new LockUnit( ResourceTypes.NODE, SHARED, 2, 2 );
        LockUnit unit3 = new LockUnit( ResourceTypes.RELATIONSHIP, SHARED, 1, 1 );
        LockUnit unit4 = new LockUnit( ResourceTypes.RELATIONSHIP, EXCLUSIVE, 2, 2 );
        LockUnit unit5 = new LockUnit( ResourceTypes.RELATIONSHIP_TYPE, SHARED, 1, 1 );

        List<LockUnit> list = asList( unit1, unit2, unit3, unit4, unit5 );
        Collections.sort( list );

        assertEquals( asList( unit1, unit4, unit2, unit3, unit5 ), list );
    }

    @Test
    void exclusiveOrderedByResourceTypes()
    {
        LockUnit unit1 = new LockUnit( ResourceTypes.NODE, EXCLUSIVE, 1, 1 );
        LockUnit unit2 = new LockUnit( ResourceTypes.RELATIONSHIP, EXCLUSIVE, 1, 1 );
        LockUnit unit3 = new LockUnit( ResourceTypes.NODE, EXCLUSIVE, 2, 2 );
        LockUnit unit4 = new LockUnit( ResourceTypes.RELATIONSHIP_TYPE, EXCLUSIVE, 1, 1 );
        LockUnit unit5 = new LockUnit( ResourceTypes.RELATIONSHIP, EXCLUSIVE, 2, 2 );

        List<LockUnit> list = asList( unit1, unit2, unit3, unit4, unit5 );
        Collections.sort( list );

        assertEquals( asList( unit1, unit3, unit2, unit5, unit4 ), list );
    }
}
