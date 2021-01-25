/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog.checktypes;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.store.record.PropertyRecord;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertyCheckTypeTest
{
    @Test
    void inUseRecordEquality()
    {
        PropertyRecord record1 = new PropertyRecord( 1 );
        record1.initialize( true, 1, 2 );
        record1.setSecondaryUnitIdOnLoad( 42 );

        PropertyRecord record2 = record1.copy();

        PropertyCheckType check = new PropertyCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    void notInUseRecordEquality()
    {
        PropertyRecord record1 = new PropertyRecord( 1 );
        record1.initialize( false, 1, 2 );
        record1.setSecondaryUnitIdOnLoad( 42 );

        PropertyRecord record2 = new PropertyRecord( 1 );
        record2.initialize( false, 11, 22 );
        record2.setSecondaryUnitIdOnLoad( 24 );

        PropertyCheckType check = new PropertyCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
