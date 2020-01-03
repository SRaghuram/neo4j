/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import co.unruly.matchers.OptionalMatchers;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZ4;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZO;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.NAME_CLASH;
import static com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.COMPRESSION;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.GRATUITOUS_OBFUSCATION;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

/**
 * @see ApplicationProtocolRepositoryTest for tests on base class
 */
public class ModifierProtocolRepositoryTest
{
    @Test
    public void shouldReturnModifierProtocolOfFirstConfiguredVersionRequestedAndSupported()
    {
        // given
        List<ModifierSupportedProtocols> supportedProtocols = asList(
                new ModifierSupportedProtocols( COMPRESSION, asList( LZO.implementation(), SNAPPY.implementation(), LZ4.implementation() ) ),
                new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, Arrays.asList( NAME_CLASH.implementation() ) ) );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), supportedProtocols );
        // when
        Optional<ModifierProtocol> modifierProtocol = modifierProtocolRepository.select(
                COMPRESSION.canonicalName(),
                asSet( "bzip2", SNAPPY.implementation(), LZ4.implementation(), LZO.implementation(), "fast_lz" )
        );

        // then
        assertThat( modifierProtocol.map( Protocol::implementation), OptionalMatchers.contains( LZO.implementation() ) );
    }

    @Test
    public void shouldReturnModifierProtocolOfSingleConfiguredVersionIfOthersRequested()
    {
        // given
        List<ModifierSupportedProtocols> supportedProtocols = asList(
                new ModifierSupportedProtocols( COMPRESSION, Arrays.asList( LZO.implementation() ) ) );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), supportedProtocols );
        // when
        Optional<ModifierProtocol> modifierProtocol =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( TestProtocols.TestModifierProtocols.allVersionsOf( COMPRESSION ) ) );

        // then
        assertThat( modifierProtocol.map( Protocol::implementation), OptionalMatchers.contains( LZO.implementation() ) );
    }

    @Test
    public void shouldCompareModifierProtocolsByListOrder()
    {
        List<ModifierSupportedProtocols> supportedProtocols = asList(
                new ModifierSupportedProtocols( COMPRESSION, asList( LZO.implementation(), SNAPPY.implementation(), LZ4.implementation() ) ) );

        Comparator<ModifierProtocol> comparator =
                ModifierProtocolRepository.getModifierProtocolComparator( supportedProtocols )
                .apply( COMPRESSION.canonicalName() );

        assertThat( comparator.compare( LZO, SNAPPY ), Matchers.greaterThan( 0 )  );
        assertThat( comparator.compare( SNAPPY, LZ4 ), Matchers.greaterThan( 0 )  );
    }
}
