/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.txlog.checktypes;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.Assert.assertTrue;

public class RelationshipCheckTypeTest
{
    @Test
    public void inUseRecordEquality()
    {
        RelationshipRecord record1 = new RelationshipRecord( 1 );
        record1.initialize( true, 1, 2, 3, 4, 5, 6, 7, 8, true, false );
        record1.setSecondaryUnitId( 42 );

        RelationshipRecord record2 = record1.clone();

        RelationshipCheckType check = new RelationshipCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    public void notInUseRecordEquality()
    {
        RelationshipRecord record1 = new RelationshipRecord( 1 );
        record1.initialize( false, 1, 2, 3, 4, 5, 6, 7, 8, true, false );
        record1.setSecondaryUnitId( 42 );

        RelationshipRecord record2 = new RelationshipRecord( 1 );
        record2.initialize( false, 11, 22, 33, 44, 55, 66, 77, 88, false, true );
        record2.setSecondaryUnitId( 24 );

        RelationshipCheckType check = new RelationshipCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
