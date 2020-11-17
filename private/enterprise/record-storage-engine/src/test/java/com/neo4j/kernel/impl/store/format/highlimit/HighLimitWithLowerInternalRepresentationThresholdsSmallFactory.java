/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.impl.store.format.RecordFormats;

@ServiceProvider
public class HighLimitWithLowerInternalRepresentationThresholdsSmallFactory implements RecordFormats.Factory
{
    public static final String NAME = HighLimit.NAME + "_lt_small";
    public static final String STORE_VERSION = "HL_lt_s";
    public static final int FIXED_REFERENCE_THRESHOLD = 2_000;
    public static final HighLimitWithLowerInternalRepresentationThresholds RECORD_FORMATS =
            new HighLimitWithLowerInternalRepresentationThresholds( NAME, STORE_VERSION, FIXED_REFERENCE_THRESHOLD, 0.6D );

    // "lt" for limited or lower threshold, both hold true
    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public RecordFormats newInstance()
    {
        return RECORD_FORMATS;
    }
}
