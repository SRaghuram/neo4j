/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.http;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.https;

public class ClientConnectorAddresses implements Iterable<ClientConnectorAddresses.ConnectorUri>
{
    private final List<ConnectorUri> connectorUris;

    public ClientConnectorAddresses( List<ConnectorUri> connectorUris )
    {
        this.connectorUris = connectorUris;
    }

    public static ClientConnectorAddresses extractFromConfig( Config config )
    {
        List<ConnectorUri> connectorUris = new ArrayList<>();

        connectorUris.add( new ConnectorUri( bolt, config.get( BoltConnector.advertised_address ) ) );

        if ( config.get( HttpConnector.enabled ) )
        {
            connectorUris.add( new ConnectorUri( http, config.get( HttpConnector.advertised_address ) ) );
        }

        if ( config.get( HttpsConnector.enabled ) )
        {
            connectorUris.add( new ConnectorUri( https, config.get( HttpsConnector.advertised_address ) ) );
        }

        return new ClientConnectorAddresses( connectorUris );
    }

    public SocketAddress boltAddress()
    {
        return connectorUris.stream().filter( connectorUri -> connectorUri.scheme == bolt ).findFirst().orElseThrow(
                () -> new IllegalArgumentException( "A Bolt connector must be configured to run a cluster" ) )
                .socketAddress;
    }

    public List<URI> uriList()
    {
        return connectorUris.stream().map( ConnectorUri::toUri ).collect( Collectors.toList() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ClientConnectorAddresses that = (ClientConnectorAddresses) o;
        return Objects.equals( connectorUris, that.connectorUris );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( connectorUris );
    }

    @Override
    public String toString()
    {
        return connectorUris.stream().map( ConnectorUri::toString ).collect( Collectors.joining( "," ) );
    }

    static ClientConnectorAddresses fromString( String value )
    {
        return new ClientConnectorAddresses( Stream.of( value.split( "," ) )
                .map( ConnectorUri::fromString ).collect( Collectors.toList() ) );
    }

    @Override
    public Iterator<ConnectorUri> iterator()
    {
        return connectorUris.iterator();
    }

    public enum Scheme
    {
        bolt, http, https
    }

    public static class ConnectorUri
    {
        private final Scheme scheme;
        private final SocketAddress socketAddress;

        public ConnectorUri( Scheme scheme, SocketAddress socketAddress )
        {
            this.scheme = scheme;
            this.socketAddress = socketAddress;
        }

        private URI toUri()
        {
            try
            {
                return new URI( scheme.name().toLowerCase(), null, socketAddress.getHostname(), socketAddress.getPort(),
                        null, null, null );
            }
            catch ( URISyntaxException e )
            {
                throw new IllegalArgumentException( e );
            }
        }

        @Override
        public String toString()
        {
            return toUri().toString();
        }

        private static ConnectorUri fromString( String string )
        {
            URI uri = URI.create( string );
            SocketAddress advertisedSocketAddress = SocketAddressParser.socketAddress( uri.getAuthority(), SocketAddress::new );
            return new ConnectorUri( Scheme.valueOf( uri.getScheme() ), advertisedSocketAddress );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ConnectorUri that = (ConnectorUri) o;
            return scheme == that.scheme &&
                    Objects.equals( socketAddress, that.socketAddress );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( scheme, socketAddress );
        }
    }

    public static class Marshal extends SafeChannelMarshal<ClientConnectorAddresses>
    {
        @Override
        protected ClientConnectorAddresses unmarshal0( ReadableChannel channel ) throws IOException
        {
            int size = channel.getInt();
            List<ConnectorUri> connectorUris = new ArrayList<>( size );
            for ( int i = 0; i < size; i++ )
            {
                String schemeName = StringMarshal.unmarshal( channel );
                String hostName = StringMarshal.unmarshal( channel );
                int port = channel.getInt();
                connectorUris.add( new ConnectorUri( Scheme.valueOf( schemeName ), new SocketAddress( hostName, port ) ) );
            }
            return new ClientConnectorAddresses( connectorUris );
        }

        @Override
        public void marshal( ClientConnectorAddresses connectorUris, WritableChannel channel ) throws IOException
        {
            channel.putInt( connectorUris.connectorUris.size() );
            for ( ConnectorUri uri : connectorUris )
            {
                StringMarshal.marshal( channel, uri.scheme.name() );
                StringMarshal.marshal( channel, uri.socketAddress.getHostname() );
                channel.putInt( uri.socketAddress.getPort() );
            }
        }
    }
}
