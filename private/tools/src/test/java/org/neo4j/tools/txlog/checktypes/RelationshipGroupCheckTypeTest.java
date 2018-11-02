/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.txlog.checktypes;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.junit.Assert.assertTrue;

public class RelationshipGroupCheckTypeTest
{
    @Test
    public void inUseRecordEquality()
    {
        RelationshipGroupRecord record1 = new RelationshipGroupRecord( 1 );
        record1.initialize( true, 1, 2, 3, 4, 5, 6 );
        record1.setSecondaryUnitId( 42 );

        RelationshipGroupRecord record2 = record1.clone();

        RelationshipGroupCheckType check = new RelationshipGroupCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    public void notInUseRecordEquality()
    {
        RelationshipGroupRecord record1 = new RelationshipGroupRecord( 1 );
        record1.initialize( false, 1, 2, 3, 4, 5, 6 );
        record1.setSecondaryUnitId( 42 );

        RelationshipGroupRecord record2 = new RelationshipGroupRecord( 1 );
        record1.initialize( false, 11, 22, 33, 44, 55, 66 );
        record2.setSecondaryUnitId( 24 );

        RelationshipGroupCheckType check = new RelationshipGroupCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
