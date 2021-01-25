/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.FormatFamily;

/**
 * High limit format family.
 * @see FormatFamily
 */
public class HighLimitFormatFamily extends FormatFamily
{
    public static final FormatFamily INSTANCE = new HighLimitFormatFamily();

    private static final String HIGH_LIMIT_FORMAT_FAMILY_NAME = "High limit format family";

    private HighLimitFormatFamily()
    {
    }

    @Override
    public String getName()
    {
        return HIGH_LIMIT_FORMAT_FAMILY_NAME;
    }

    @Override
    public int rank()
    {
        return 2;
    }

}
