/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory;
import com.neo4j.configuration.ApplicationProtocolVersion;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public interface TestProtocols
{
    static <U extends Comparable<U>,T extends Protocol<U>> T latest( Protocol.Category<T> category, T[] values )
    {
        return Stream.of( values )
                .filter( protocol -> protocol.category().equals( category.canonicalName() ) )
                .max( Comparator.comparing( T::implementation ) )
                .orElseThrow();
    }

    static <U extends Comparable<U>, T extends Protocol<U>> U[] allVersionsOf( Protocol.Category<T> category, T[] values, IntFunction<U[]> constructor )
    {
        return Stream.of( values )
                .filter( protocol -> protocol.category().equals( category.canonicalName() ) )
                .map( Protocol::implementation )
                .toArray( constructor );
    }

    enum TestApplicationProtocols implements ApplicationProtocol
    {
        RAFT_1( ApplicationProtocolCategory.RAFT, new ApplicationProtocolVersion( 1, 0 ) ),
        RAFT_2( ApplicationProtocolCategory.RAFT, new ApplicationProtocolVersion( 2, 0 ) ),
        RAFT_3( ApplicationProtocolCategory.RAFT, new ApplicationProtocolVersion( 3, 0 ) ),
        RAFT_4( ApplicationProtocolCategory.RAFT, new ApplicationProtocolVersion( 4, 0 ) ),
        CATCHUP_1( ApplicationProtocolCategory.CATCHUP, new ApplicationProtocolVersion( 1, 0 ) ),
        CATCHUP_2( ApplicationProtocolCategory.CATCHUP, new ApplicationProtocolVersion( 2, 0 ) ),
        CATCHUP_3( ApplicationProtocolCategory.CATCHUP, new ApplicationProtocolVersion( 3, 0 ) ),
        CATCHUP_4( ApplicationProtocolCategory.CATCHUP, new ApplicationProtocolVersion( 4, 0 ) );

        private final ApplicationProtocolVersion version;
        private final ApplicationProtocolCategory identifier;

        TestApplicationProtocols( ApplicationProtocolCategory identifier, ApplicationProtocolVersion version )
        {
            this.identifier = identifier;
            this.version = version;
        }

        @Override
        public String category()
        {
            return this.identifier.canonicalName();
        }

        @Override
        public ApplicationProtocolVersion implementation()
        {
            return version;
        }

        public static ApplicationProtocol latest( ApplicationProtocolCategory identifier )
        {
            return TestProtocols.latest( identifier, values() );
        }

        public static ApplicationProtocolVersion[] allVersionsOf( ApplicationProtocolCategory identifier )
        {
            return TestProtocols.allVersionsOf( identifier, TestApplicationProtocols.values(), ApplicationProtocolVersion[]::new );
        }

        public static List<ApplicationProtocolVersion> listVersionsOf( ApplicationProtocolCategory identifier )
        {
            return Arrays.asList( allVersionsOf( identifier ) );
        }
    }

    enum TestModifierProtocols implements ModifierProtocol
    {
        SNAPPY( ModifierProtocolCategory.COMPRESSION, "TestSnappy" ),
        LZO( ModifierProtocolCategory.COMPRESSION, "TestLZO" ),
        LZ4( ModifierProtocolCategory.COMPRESSION, "TestLZ4" ),
        LZ4_VALIDATING( ModifierProtocolCategory.COMPRESSION, "TestLZ4Validating" ),
        LZ4_HIGH_COMPRESSION( ModifierProtocolCategory.COMPRESSION, "TestLZ4High" ),
        LZ4_HIGH_COMPRESSION_VALIDATING( ModifierProtocolCategory.COMPRESSION, "TestLZ4HighValidating" ),
        ROT13( ModifierProtocolCategory.GRATUITOUS_OBFUSCATION, "ROT13" ),
        NAME_CLASH( ModifierProtocolCategory.GRATUITOUS_OBFUSCATION, "TestSnappy" );

        private final ModifierProtocolCategory identifier;
        private final String friendlyName;

        TestModifierProtocols( ModifierProtocolCategory identifier, String friendlyName )
        {
            this.identifier = identifier;
            this.friendlyName = friendlyName;
        }

        @Override
        public String category()
        {
            return identifier.canonicalName();
        }

        @Override
        public String implementation()
        {
            return friendlyName;
        }

        public static ModifierProtocol latest( ModifierProtocolCategory identifier )
        {
            return TestProtocols.latest( identifier, values() );
        }

        public static String[] allVersionsOf( ModifierProtocolCategory identifier )
        {
            return TestProtocols.allVersionsOf( identifier, TestModifierProtocols.values(), String[]::new );
        }

        public static List<String> listVersionsOf( ModifierProtocolCategory identifier )
        {
            List<String> versions = Arrays.asList( allVersionsOf( identifier ) );
            versions.sort( Comparator.reverseOrder() );
            return versions;
        }
    }
}
