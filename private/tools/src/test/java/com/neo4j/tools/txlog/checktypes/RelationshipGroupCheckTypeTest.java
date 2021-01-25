/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog.checktypes;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipGroupCheckTypeTest
{
    @Test
    void inUseRecordEquality()
    {
        RelationshipGroupRecord record1 = new RelationshipGroupRecord( 1 );
        record1.initialize( true, 1, 2, 3, 4, 5, 6 );
        record1.setSecondaryUnitIdOnLoad( 42 );

        RelationshipGroupRecord record2 = record1.copy();

        RelationshipGroupCheckType check = new RelationshipGroupCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    void notInUseRecordEquality()
    {
        RelationshipGroupRecord record1 = new RelationshipGroupRecord( 1 );
        record1.initialize( false, 1, 2, 3, 4, 5, 6 );
        record1.setSecondaryUnitIdOnLoad( 42 );

        RelationshipGroupRecord record2 = new RelationshipGroupRecord( 1 );
        record1.initialize( false, 11, 22, 33, 44, 55, 66 );
        record2.setSecondaryUnitIdOnLoad( 24 );

        RelationshipGroupCheckType check = new RelationshipGroupCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
