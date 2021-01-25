/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit.v30;

import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;

import org.neo4j.kernel.impl.store.format.AbstractRecordFormatTest;

public class HighLimitV3_4RecordFormatTest extends AbstractRecordFormatTest
{
    public HighLimitV3_4RecordFormatTest()
    {
        super( HighLimitV3_4_0.RECORD_FORMATS, 50, 50 );
    }
}
