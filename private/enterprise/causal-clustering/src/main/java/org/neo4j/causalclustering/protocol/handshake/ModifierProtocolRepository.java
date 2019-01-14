/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.protocol.Protocol;

public class ModifierProtocolRepository extends ProtocolRepository<String,Protocol.ModifierProtocol>
{
    private final Collection<ModifierSupportedProtocols> supportedProtocols;
    private final Map<String,ModifierSupportedProtocols> supportedProtocolsLookup;

    public ModifierProtocolRepository( Protocol.ModifierProtocol[] protocols, Collection<ModifierSupportedProtocols> supportedProtocols )
    {
        super( protocols, getModifierProtocolComparator( supportedProtocols ), ModifierProtocolSelection::new );
        this.supportedProtocols = Collections.unmodifiableCollection( supportedProtocols );
        this.supportedProtocolsLookup = supportedProtocols.stream()
                .collect( Collectors.toMap( supp -> supp.identifier().canonicalName(), Function.identity() ) );
    }

    static Function<String,Comparator<Protocol.ModifierProtocol>> getModifierProtocolComparator(
            Collection<ModifierSupportedProtocols> supportedProtocols )
    {
        return getModifierProtocolComparator( versionMap( supportedProtocols ) );
    }

    private static Map<String,List<String>> versionMap( Collection<ModifierSupportedProtocols> supportedProtocols )
    {
        return supportedProtocols.stream()
                .collect( Collectors.toMap( supportedProtocol -> supportedProtocol.identifier().canonicalName(), SupportedProtocols::versions ) );
    }

    private static Function<String,Comparator<Protocol.ModifierProtocol>> getModifierProtocolComparator( Map<String,List<String>> versionMap )
    {
        return protocolName -> {
            Comparator<Protocol.ModifierProtocol> positionalComparator = Comparator.comparing( modifierProtocol ->
                    Optional.ofNullable( versionMap.get( protocolName ) )
                    .map( versions -> byPosition( modifierProtocol, versions ) )
                    .orElse( 0 ) );

            return fallBackToVersionNumbers( positionalComparator );
        };
    }

    // Needed if supported modifiers has an empty version list
    private static Comparator<Protocol.ModifierProtocol> fallBackToVersionNumbers( Comparator<Protocol.ModifierProtocol> positionalComparator )
    {
        return positionalComparator.thenComparing( versionNumberComparator() );
    }

    /**
     * @return Greatest is head of versions, least is not included in versions
     */
    private static Integer byPosition( Protocol.ModifierProtocol modifierProtocol, List<String> versions )
    {
        int index = versions.indexOf( modifierProtocol.implementation() );
        return index == -1 ? Integer.MIN_VALUE : -index;
    }

    public Optional<SupportedProtocols<String,Protocol.ModifierProtocol>> supportedProtocolFor( String protocolName )
    {
        return Optional.ofNullable( supportedProtocolsLookup.get( protocolName ) );
    }

    public Collection<ModifierSupportedProtocols> supportedProtocols()
    {
        return supportedProtocols;
    }
}
