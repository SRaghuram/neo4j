/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class ProtocolInstallerRepository<O extends ProtocolInstaller.Orientation>
{
    private final Map<ApplicationProtocol,ProtocolInstaller.Factory<O,?>> installers;
    private final Map<ModifierProtocol,ModifierProtocolInstaller<O>> modifiers;

    public ProtocolInstallerRepository( Collection<ProtocolInstaller.Factory<O, ?>> installers, Collection<ModifierProtocolInstaller<O>> modifiers )
    {
        Map<ApplicationProtocol,ProtocolInstaller.Factory<O,?>> tempInstallers = new HashMap<>();
        installers.forEach( installer -> addTo( tempInstallers, installer, installer.applicationProtocol() ) );
        this.installers = unmodifiableMap( tempInstallers );

        Map<ModifierProtocol,ModifierProtocolInstaller<O>> tempModifierInstallers = new HashMap<>();
        modifiers.forEach( installer -> installer.protocols().forEach( protocol -> addTo( tempModifierInstallers, installer, protocol ) ) );
        this.modifiers = unmodifiableMap( tempModifierInstallers );
    }

    private <T, P extends Protocol> void addTo( Map<P,T> tempServerMap, T installer, P protocol )
    {
        T old = tempServerMap.put( protocol, installer );
        if ( old != null )
        {
            throw new IllegalArgumentException(
                    String.format( "Duplicate protocol installers for protocol %s: %s and %s", protocol, installer, old )
            );
        }
    }

    public ProtocolInstaller<O> installerFor( ProtocolStack protocolStack )
    {
        ApplicationProtocol applicationProtocol = protocolStack.applicationProtocol();
        ProtocolInstaller.Factory<O,?> protocolInstaller = installers.get( applicationProtocol );

        ensureKnownProtocol( applicationProtocol, protocolInstaller );

        return protocolInstaller.create( getModifierProtocolInstallers( protocolStack ) );
    }

    private List<ModifierProtocolInstaller<O>> getModifierProtocolInstallers( ProtocolStack protocolStack )
    {
        List<ModifierProtocolInstaller<O>> modifierProtocolInstallers = new ArrayList<>();
        for ( ModifierProtocol modifierProtocol : protocolStack.modifierProtocols() )
        {
            ensureNotDuplicate( modifierProtocolInstallers, modifierProtocol );

            ModifierProtocolInstaller<O> protocolInstaller = modifiers.get( modifierProtocol );

            ensureKnownProtocol( modifierProtocol, protocolInstaller );

            modifierProtocolInstallers.add( protocolInstaller );
        }
        return modifierProtocolInstallers;
    }

    private void ensureNotDuplicate( List<ModifierProtocolInstaller<O>> modifierProtocolInstallers, ModifierProtocol modifierProtocol )
    {
        boolean duplicateIdentifier = modifierProtocolInstallers
                .stream()
                .flatMap( modifier -> modifier.protocols().stream() )
                .anyMatch( protocol -> protocol.category().equals( modifierProtocol.category() ) );
        if ( duplicateIdentifier )
        {
            throw new IllegalArgumentException( "Attempted to install multiple versions of " + modifierProtocol.category() );
        }
    }

    private void ensureKnownProtocol( Protocol protocol, Object protocolInstaller )
    {
        if ( protocolInstaller == null )
        {
            throw new IllegalStateException( String.format( "Installer for requested protocol %s does not exist", protocol ) );
        }
    }
}
