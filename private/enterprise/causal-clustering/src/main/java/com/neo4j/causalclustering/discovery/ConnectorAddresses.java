/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeChannelMarshal;

import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.http;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.https;

public class ConnectorAddresses
{
    private final ConnectorUri defaultClientBoltAddress;
    private final ConnectorUri defaultIntraClusterBoltAddress;
    private final ConnectorUri defaultHttpAddress;
    private final ConnectorUri defaultHttpsAddress;
    private final List<ConnectorUri> excessConnectorUris;

    private ConnectorAddresses( ConnectorUri clientBoltAddress, ConnectorUri intraClusterBoltAddress,
                                ConnectorUri httpAddress, ConnectorUri httpsAddress, List<ConnectorUri> excessConnectorUris )
    {
        this.defaultClientBoltAddress = clientBoltAddress;
        this.defaultIntraClusterBoltAddress = intraClusterBoltAddress;
        this.defaultHttpAddress = httpAddress;
        this.defaultHttpsAddress = httpsAddress;
        this.excessConnectorUris = excessConnectorUris;
    }

    public static ConnectorAddresses fromList( List<ConnectorUri> connectorUris )
    {
        ConnectorUri clientBoltAddress = null;
        ConnectorUri intraClusterBoltAddress = null;
        ConnectorUri httpAddress = null;
        ConnectorUri httpsAddress = null;

        var excessConnectorUris = new ArrayList<ConnectorUri>();

        for ( ConnectorUri uri : connectorUris )
        {
            if ( uri.scheme == bolt && clientBoltAddress == null )
            {
                // By convention, the first bolt address in a list is the default client facing bolt connector
                clientBoltAddress = uri;
            }
            else if ( uri.scheme == bolt && intraClusterBoltAddress == null )
            {
                // By convention, the second bolt address in a list is the default intra-cluster bolt connector
                intraClusterBoltAddress = uri;
            }
            else if ( uri.scheme == http && httpAddress == null )
            {
                // By convention, the first http address in a list is the default
                httpAddress = uri;
            }
            else if ( uri.scheme == https && httpsAddress == null )
            {
                // By convention, the first https address in a list is the default
                httpsAddress = uri;
            }
            else
            {
                excessConnectorUris.add( uri );
            }
        }

        return new ConnectorAddresses( clientBoltAddress, intraClusterBoltAddress, httpAddress, httpsAddress, excessConnectorUris );
    }

    public static ConnectorAddresses fromConfig( Config config )
    {
        ConnectorUri clientBoltAddress;
        ConnectorUri intraClusterBoltAddress = null;
        ConnectorUri httpAddress = null;
        ConnectorUri httpsAddress = null;

        clientBoltAddress = new ConnectorUri( bolt, config.get( BoltConnector.advertised_address ) );

        if ( config.get( GraphDatabaseSettings.routing_enabled ) )
        {
            intraClusterBoltAddress = new ConnectorUri( bolt, config.get( GraphDatabaseSettings.routing_advertised_address ) );
        }

        if ( config.get( HttpConnector.enabled ) )
        {
            httpAddress = new ConnectorUri( http, config.get( HttpConnector.advertised_address ) );
        }

        if ( config.get( HttpsConnector.enabled ) )
        {
            httpsAddress = new ConnectorUri( https, config.get( HttpsConnector.advertised_address ) );
        }

        return new ConnectorAddresses( clientBoltAddress, intraClusterBoltAddress, httpAddress, httpsAddress, List.of() );
    }

    public SocketAddress clientBoltAddress()
    {
        if ( defaultClientBoltAddress == null )
        {
            throw new IllegalArgumentException( "A Bolt connector must be configured to run a cluster" );
        }
        return defaultClientBoltAddress.socketAddress;
    }

    public Optional<SocketAddress> intraClusterBoltAddress()
    {
        return Optional.ofNullable( defaultIntraClusterBoltAddress )
                       .map( ConnectorUri::socketAddress );
    }

    public List<String> publicUriList()
    {
        return orderedConnectors()
                     .filter( uri -> !Objects.equals( uri, defaultIntraClusterBoltAddress ) )
                     .map( ConnectorUri::toString )
                     .collect( Collectors.toList() );
    }

    /**
     * N.B. the order of connectors in this method is important.
     * @return All connectors stored in this instance.
     */
    private Stream<ConnectorUri> orderedConnectors()
    {
        var defaultConnectors = Stream.of( defaultClientBoltAddress,
                                           defaultIntraClusterBoltAddress,
                                           defaultHttpAddress,
                                           defaultHttpsAddress );

        return Stream.concat( defaultConnectors.filter( Objects::nonNull ), excessConnectorUris.stream() );
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
        ConnectorAddresses that = (ConnectorAddresses) o;
        return Objects.equals( defaultClientBoltAddress, that.defaultClientBoltAddress ) &&
               Objects.equals( defaultIntraClusterBoltAddress, that.defaultIntraClusterBoltAddress ) &&
               Objects.equals( defaultHttpAddress, that.defaultHttpAddress ) &&
               Objects.equals( defaultHttpsAddress, that.defaultHttpsAddress ) &&
               Objects.equals( excessConnectorUris, that.excessConnectorUris );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( defaultClientBoltAddress, defaultIntraClusterBoltAddress, defaultHttpAddress, defaultHttpsAddress, excessConnectorUris );
    }

    @Override
    public String toString()
    {
        return orderedConnectors().map( ConnectorUri::toString ).collect( Collectors.joining( "," ) );
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

        private SocketAddress socketAddress()
        {
            return socketAddress;
        }

        @Override
        public String toString()
        {
            return String.format( "%s://%s", scheme.name().toLowerCase(), SocketAddress.format(  socketAddress.getHostname(), socketAddress.getPort() ) );
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

    public static class Marshal extends SafeChannelMarshal<ConnectorAddresses>
    {
        @Override
        protected ConnectorAddresses unmarshal0( ReadableChannel channel ) throws IOException
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
            return ConnectorAddresses.fromList( connectorUris );
        }

        @Override
        public void marshal( ConnectorAddresses connectorUris, WritableChannel channel ) throws IOException
        {
            var allUris = connectorUris.orderedConnectors().collect( Collectors.toList() );
            channel.putInt( allUris.size() );
            for ( ConnectorUri uri : allUris )
            {
                StringMarshal.marshal( channel, uri.scheme.name() );
                StringMarshal.marshal( channel, uri.socketAddress.getHostname() );
                channel.putInt( uri.socketAddress.getPort() );
            }
        }
    }
}
