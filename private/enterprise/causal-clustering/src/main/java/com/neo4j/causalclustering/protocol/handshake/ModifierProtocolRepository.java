/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ModifierProtocolRepository extends ProtocolRepository<String,ModifierProtocol>
{
    private final Collection<ModifierSupportedProtocols> supportedProtocols;
    private final Map<String,ModifierSupportedProtocols> supportedProtocolsLookup;

    public ModifierProtocolRepository( ModifierProtocol[] protocols, Collection<ModifierSupportedProtocols> supportedProtocols )
    {
        super( protocols, getModifierProtocolComparator( supportedProtocols ), ModifierProtocolSelection::new );
        this.supportedProtocols = Collections.unmodifiableCollection( supportedProtocols );
        this.supportedProtocolsLookup = supportedProtocols.stream()
                .collect( Collectors.toMap( supp -> supp.identifier().canonicalName(), Function.identity() ) );
    }

    static Function<String,Comparator<ModifierProtocol>> getModifierProtocolComparator(
            Collection<ModifierSupportedProtocols> supportedProtocols )
    {
        return getModifierProtocolComparator( versionMap( supportedProtocols ) );
    }

    private static Map<String,List<String>> versionMap( Collection<ModifierSupportedProtocols> supportedProtocols )
    {
        return supportedProtocols.stream()
                .collect( Collectors.toMap( supportedProtocol -> supportedProtocol.identifier().canonicalName(), SupportedProtocols::versions ) );
    }

    private static Function<String,Comparator<ModifierProtocol>> getModifierProtocolComparator( Map<String,List<String>> versionMap )
    {
        return protocolName -> {
            Comparator<ModifierProtocol> positionalComparator = Comparator.comparing( modifierProtocol ->
                    Optional.ofNullable( versionMap.get( protocolName ) )
                    .map( versions -> byPosition( modifierProtocol, versions ) )
                    .orElse( 0 ) );

            return fallBackToVersionNumbers( positionalComparator );
        };
    }

    // Needed if supported modifiers has an empty version list
    private static Comparator<ModifierProtocol> fallBackToVersionNumbers( Comparator<ModifierProtocol> positionalComparator )
    {
        return positionalComparator.thenComparing( versionNumberComparator() );
    }

    /**
     * @return Greatest is head of versions, least is not included in versions
     */
    private static Integer byPosition( ModifierProtocol modifierProtocol, List<String> versions )
    {
        int index = versions.indexOf( modifierProtocol.implementation() );
        return index == -1 ? Integer.MIN_VALUE : -index;
    }

    public Optional<SupportedProtocols<String,ModifierProtocol>> supportedProtocolFor( String protocolName )
    {
        return Optional.ofNullable( supportedProtocolsLookup.get( protocolName ) );
    }

    public Collection<ModifierSupportedProtocols> supportedProtocols()
    {
        return supportedProtocols;
    }
}
