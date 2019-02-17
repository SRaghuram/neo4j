/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit.v320;

import org.neo4j.kernel.impl.store.format.RecordFormats;

public class HighLimitFactoryV3_2_0 extends RecordFormats.Factory
{
    public HighLimitFactoryV3_2_0()
    {
        super( HighLimitV3_2_0.NAME, HighLimitV3_2_0.STORE_VERSION );
    }

    @Override
    public RecordFormats newInstance()
    {
        return HighLimitV3_2_0.RECORD_FORMATS;
    }
}
