/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.Channel;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import com.neo4j.configuration.ApplicationProtocolVersion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.neo4j.internal.helpers.collection.Pair;

public class HandshakeClient implements ClientMessageHandler
{
    private Channel channel;
    private ApplicationProtocolRepository applicationProtocolRepository;
    private ApplicationSupportedProtocols supportedApplicationProtocol;
    private ModifierProtocolRepository modifierProtocolRepository;
    private Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private ApplicationProtocol negotiatedApplicationProtocol;
    private List<Pair<String,Optional<ModifierProtocol>>> negotiatedModifierProtocols;
    private ProtocolStack protocolStack;

    private final CompletableFuture<ProtocolStack> fProtocol;

    HandshakeClient( CompletableFuture<ProtocolStack> protocolFuture )
    {
        this.fProtocol = protocolFuture;
    }

    public void initiate( Channel channel,
            ApplicationProtocolRepository applicationProtocolRepository,
            ModifierProtocolRepository modifierProtocolRepository )
    {
        this.channel = channel;

        this.applicationProtocolRepository = applicationProtocolRepository;
        this.supportedApplicationProtocol = applicationProtocolRepository.supportedProtocol();

        this.modifierProtocolRepository = modifierProtocolRepository;
        this.supportedModifierProtocols = modifierProtocolRepository.supportedProtocols();

        negotiatedModifierProtocols = new ArrayList<>( supportedModifierProtocols.size() );

        sendProtocolRequests( channel, supportedApplicationProtocol, supportedModifierProtocols );
    }

    private void sendProtocolRequests( Channel channel, ApplicationSupportedProtocols applicationProtocols,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols )
    {
        supportedModifierProtocols.forEach( modifierProtocol ->
                {
                    ProtocolSelection<String,ModifierProtocol> protocolSelection =
                            modifierProtocolRepository.getAll( modifierProtocol.identifier(), modifierProtocol.versions() );
                    channel.write( new ModifierProtocolRequest( protocolSelection.identifier(), protocolSelection.versions() ) );
                } );

        ProtocolSelection<ApplicationProtocolVersion,ApplicationProtocol> applicationProtocolSelection =
                applicationProtocolRepository.getAll( applicationProtocols.identifier(), applicationProtocols.versions() );
        channel.writeAndFlush( new ApplicationProtocolRequest( applicationProtocolSelection.identifier(), applicationProtocolSelection.versions() ) );
    }

    @Override
    public void handle( ApplicationProtocolResponse applicationProtocolResponse )
    {
        if ( applicationProtocolResponse.statusCode() != StatusCode.SUCCESS )
        {
            fail( "Unsuccessful application protocol response" );
            return;
        }

        Optional<ApplicationProtocol> protocol =
                applicationProtocolRepository.select( applicationProtocolResponse.protocolName(), applicationProtocolResponse.version() );

        if ( protocol.isEmpty() )
        {
            ProtocolSelection<ApplicationProtocolVersion,ApplicationProtocol> knownApplicationProtocolVersions =
                    applicationProtocolRepository.getAll( supportedApplicationProtocol.identifier(), supportedApplicationProtocol.versions() );
            fail( String.format(
                    "Mismatch of application protocols between client and server: Server protocol %s version %s: Client protocol %s versions %s",
                    applicationProtocolResponse.protocolName(), applicationProtocolResponse.version(),
                    knownApplicationProtocolVersions.identifier(), knownApplicationProtocolVersions.versions() ) );
        }
        else
        {
            negotiatedApplicationProtocol = protocol.get();

            sendSwitchOverRequestIfReady();
        }
    }

    @Override
    public void handle( ModifierProtocolResponse modifierProtocolResponse )
    {
        if ( modifierProtocolResponse.statusCode() == StatusCode.SUCCESS )
        {
            Optional<ModifierProtocol> selectedModifierProtocol =
                    modifierProtocolRepository.select( modifierProtocolResponse.protocolName(), modifierProtocolResponse.version() );
            negotiatedModifierProtocols.add( Pair.of( modifierProtocolResponse.protocolName(), selectedModifierProtocol ) );
        }
        else
        {
            negotiatedModifierProtocols.add( Pair.of( modifierProtocolResponse.protocolName(), Optional.empty() ) );
        }

        sendSwitchOverRequestIfReady();
    }

    private void sendSwitchOverRequestIfReady()
    {
        if ( negotiatedApplicationProtocol != null && negotiatedModifierProtocols.size() == supportedModifierProtocols.size() )
        {
            List<ModifierProtocol> agreedModifierProtocols = negotiatedModifierProtocols
                    .stream()
                    .map( Pair::other )
                    .flatMap( Optional::stream )
                    .collect( Collectors.toList() );

            protocolStack = new ProtocolStack( negotiatedApplicationProtocol, agreedModifierProtocols );
            List<Pair<String,String>> switchOverModifierProtocols =
                    agreedModifierProtocols
                            .stream()
                            .map( protocol -> Pair.of( protocol.category(), protocol.implementation() ) )
                            .collect( Collectors.toList() );

            channel.writeAndFlush(
                    new SwitchOverRequest(
                            negotiatedApplicationProtocol.category(),
                            negotiatedApplicationProtocol.implementation(),
                            switchOverModifierProtocols ) );
        }
    }

    @Override
    public void handle( SwitchOverResponse response )
    {
        if ( protocolStack == null )
        {
            fail( "Attempted to switch over when protocol stack not established" );
        }
        else if ( response.status() != StatusCode.SUCCESS )
        {
            fail( "Server failed to switch over" );
        }
        else
        {
            fProtocol.complete( protocolStack );
        }
    }

    private void fail( String message )
    {
        fProtocol.completeExceptionally( new ClientHandshakeException( message, negotiatedApplicationProtocol, negotiatedModifierProtocols ) );
    }

    public CompletableFuture<ProtocolStack> protocol()
    {
        return fProtocol;
    }
}
