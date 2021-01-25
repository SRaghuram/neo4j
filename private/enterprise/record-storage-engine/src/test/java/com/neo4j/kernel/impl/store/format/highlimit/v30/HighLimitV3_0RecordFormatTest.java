/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit.v30;

import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;

import org.neo4j.kernel.impl.store.format.AbstractRecordFormatTest;

public class HighLimitV3_0RecordFormatTest extends AbstractRecordFormatTest
{
    public HighLimitV3_0RecordFormatTest()
    {
        super( HighLimitV3_0_0.RECORD_FORMATS, 50, 50 );
    }
}
