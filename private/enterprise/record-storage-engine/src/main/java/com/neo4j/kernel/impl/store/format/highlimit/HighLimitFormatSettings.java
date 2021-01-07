/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;

/**
 * Reference class for high limit format settings.
 *
 * @see HighLimit
 */
public class HighLimitFormatSettings
{
    /**
     * Default maximum number of bits that can be used to represent id
     */
    public static final int DEFAULT_MAXIMUM_BITS_PER_ID = 50;

    public static final int PROPERTY_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    public static final int NODE_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    public static final int RELATIONSHIP_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    public static final int RELATIONSHIP_GROUP_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;
    public static final int DYNAMIC_MAXIMUM_ID_BITS = DEFAULT_MAXIMUM_BITS_PER_ID;

    @SuppressWarnings( "unused" )
    public static final int PROPERTY_TOKEN_MAXIMUM_ID_BITS = StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS;
    @SuppressWarnings( "unused" )
    public static final int LABEL_TOKEN_MAXIMUM_ID_BITS = StandardFormatSettings.LABEL_TOKEN_MAXIMUM_ID_BITS;
    @SuppressWarnings( "unused" )
    public static final int RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS = Byte.SIZE * 3;

    private HighLimitFormatSettings()
    {
    }
}
