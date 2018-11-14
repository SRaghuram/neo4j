/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LockUnitTest
{
    @Test
    void exclusiveLocksAppearFirst()
    {
        LockUnit unit1 = new LockUnit( ResourceTypes.NODE, 1, true );
        LockUnit unit2 = new LockUnit( ResourceTypes.NODE, 2, false );
        LockUnit unit3 = new LockUnit( ResourceTypes.RELATIONSHIP, 1, false );
        LockUnit unit4 = new LockUnit( ResourceTypes.RELATIONSHIP, 2, true );
        LockUnit unit5 = new LockUnit( ResourceTypes.RELATIONSHIP_TYPE, 1, false );

        List<LockUnit> list = asList( unit1, unit2, unit3, unit4, unit5 );
        Collections.sort( list );

        assertEquals( asList( unit1, unit4, unit2, unit3, unit5 ), list );
    }

    @Test
    void exclusiveOrderedByResourceTypes()
    {
        LockUnit unit1 = new LockUnit( ResourceTypes.NODE, 1, true );
        LockUnit unit2 = new LockUnit( ResourceTypes.RELATIONSHIP, 1, true );
        LockUnit unit3 = new LockUnit( ResourceTypes.NODE, 2, true );
        LockUnit unit4 = new LockUnit( ResourceTypes.RELATIONSHIP_TYPE, 1, true );
        LockUnit unit5 = new LockUnit( ResourceTypes.RELATIONSHIP, 2, true );

        List<LockUnit> list = asList( unit1, unit2, unit3, unit4, unit5 );
        Collections.sort( list );

        assertEquals( asList( unit1, unit3, unit2, unit5, unit4 ), list );
    }
}
