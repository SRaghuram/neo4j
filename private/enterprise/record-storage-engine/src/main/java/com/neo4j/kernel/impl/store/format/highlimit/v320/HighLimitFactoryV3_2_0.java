/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit.v320;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.impl.store.format.RecordFormats;

@ServiceProvider
public class HighLimitFactoryV3_2_0 implements RecordFormats.Factory
{
    @Override
    public String getName()
    {
        return HighLimitV3_2_0.NAME;
    }

    @Override
    public RecordFormats newInstance()
    {
        return HighLimitV3_2_0.RECORD_FORMATS;
    }
}
