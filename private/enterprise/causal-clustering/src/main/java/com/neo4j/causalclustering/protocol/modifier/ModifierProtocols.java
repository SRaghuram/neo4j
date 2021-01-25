/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.modifier;

import com.neo4j.causalclustering.protocol.Protocol;

import java.util.Optional;

import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.COMPRESSION;

public enum ModifierProtocols implements ModifierProtocol
{
    COMPRESSION_GZIP( COMPRESSION, Implementations.GZIP ),
    COMPRESSION_SNAPPY( COMPRESSION, Implementations.SNAPPY ),
    COMPRESSION_SNAPPY_VALIDATING( COMPRESSION, Implementations.SNAPPY_VALIDATING ),
    COMPRESSION_LZ4( COMPRESSION, Implementations.LZ4 ),
    COMPRESSION_LZ4_HIGH_COMPRESSION( COMPRESSION, Implementations.LZ4_HIGH_COMPRESSION ),
    COMPRESSION_LZ4_VALIDATING( COMPRESSION, Implementations.LZ_VALIDATING ),
    COMPRESSION_LZ4_HIGH_COMPRESSION_VALIDATING( COMPRESSION, Implementations.LZ4_HIGH_COMPRESSION_VALIDATING );

    public static final String ALLOWED_VALUES_STRING = "[" + Implementations.GZIP + ", " + Implementations.SNAPPY + ", " +
                                                       Implementations.SNAPPY_VALIDATING + ", " + Implementations.LZ4 + ", " +
                                                       Implementations.LZ4_HIGH_COMPRESSION + ", " + Implementations.LZ_VALIDATING + ", " +
                                                       Implementations.LZ4_HIGH_COMPRESSION_VALIDATING + "]";

    // Should be human writable into a comma separated list
    private final String friendlyName;
    private final ModifierProtocolCategory identifier;

    ModifierProtocols( ModifierProtocolCategory identifier, String friendlyName )
    {
        this.identifier = identifier;
        this.friendlyName = friendlyName;
    }

    @Override
    public String implementation()
    {
        return friendlyName;
    }

    @Override
    public String category()
    {
        return identifier.canonicalName();
    }

    public static Optional<ModifierProtocol> find( ModifierProtocolCategory category, String friendlyName )
    {
        return Protocol.find( com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.values(), category, friendlyName, String::toLowerCase );
    }

    private static class Implementations
    {
        static final String GZIP = "Gzip";
        static final String SNAPPY = "Snappy";
        static final String SNAPPY_VALIDATING = "Snappy_validating";
        static final String LZ4 = "LZ4";
        static final String LZ4_HIGH_COMPRESSION = "LZ4_high_compression";
        static final String LZ_VALIDATING = "LZ_validating";
        static final String LZ4_HIGH_COMPRESSION_VALIDATING = "LZ4_high_compression_validating";
    }
}
