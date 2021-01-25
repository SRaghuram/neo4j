/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog.checktypes;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipCheckTypeTest
{
    @Test
    void inUseRecordEquality()
    {
        RelationshipRecord record1 = new RelationshipRecord( 1 );
        record1.initialize( true, 1, 2, 3, 4, 5, 6, 7, 8, true, false );
        record1.setSecondaryUnitIdOnLoad( 42 );

        RelationshipRecord record2 = record1.copy();

        RelationshipCheckType check = new RelationshipCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    void notInUseRecordEquality()
    {
        RelationshipRecord record1 = new RelationshipRecord( 1 );
        record1.initialize( false, 1, 2, 3, 4, 5, 6, 7, 8, true, false );
        record1.setSecondaryUnitIdOnLoad( 42 );

        RelationshipRecord record2 = new RelationshipRecord( 1 );
        record2.initialize( false, 11, 22, 33, 44, 55, 66, 77, 88, false, true );
        record2.setSecondaryUnitIdOnLoad( 24 );

        RelationshipCheckType check = new RelationshipCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
