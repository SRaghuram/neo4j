/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.txlog.checktypes;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.Assert.assertTrue;

public class NodeCheckTypeTest
{
    @Test
    public void inUseRecordEquality()
    {
        NodeRecord record1 = new NodeRecord( 1 );
        record1.initialize( true, 1, false, 2, 3 );
        record1.setSecondaryUnitId( 42 );

        NodeRecord record2 = record1.clone();

        NodeCheckType check = new NodeCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    public void notInUseRecordEquality()
    {
        NodeRecord record1 = new NodeRecord( 1 );
        record1.initialize( false, 1, true, 2, 3 );
        record1.setSecondaryUnitId( 42 );

        NodeRecord record2 = new NodeRecord( 1 );
        record2.initialize( false, 11, true, 22, 33 );
        record2.setSecondaryUnitId( 24 );

        NodeCheckType check = new NodeCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
