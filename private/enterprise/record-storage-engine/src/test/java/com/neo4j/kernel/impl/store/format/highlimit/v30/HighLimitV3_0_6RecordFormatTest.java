/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit.v30;

import com.neo4j.kernel.impl.store.format.highlimit.v306.HighLimitV3_0_6;

import org.neo4j.kernel.impl.store.format.AbstractRecordFormatTest;

public class HighLimitV3_0_6RecordFormatTest extends AbstractRecordFormatTest
{
    public HighLimitV3_0_6RecordFormatTest()
    {
        super( HighLimitV3_0_6.RECORD_FORMATS, 50, 50 );
    }
}
