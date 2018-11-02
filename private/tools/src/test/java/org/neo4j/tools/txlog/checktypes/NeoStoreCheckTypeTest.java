/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.txlog.checktypes;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NeoStoreRecord;

import static org.junit.Assert.assertTrue;

public class NeoStoreCheckTypeTest
{
    @Test
    public void inUseRecordEquality()
    {
        NeoStoreRecord record1 = new NeoStoreRecord();
        record1.initialize( true, 1 );

        NeoStoreRecord record2 = record1.clone();

        NeoStoreCheckType check = new NeoStoreCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    public void notInUseRecordEquality()
    {
        NeoStoreRecord record1 = new NeoStoreRecord();
        record1.initialize( false, 1 );

        NeoStoreRecord record2 = new NeoStoreRecord();
        record2.initialize( false, 11 );

        NeoStoreCheckType check = new NeoStoreCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
