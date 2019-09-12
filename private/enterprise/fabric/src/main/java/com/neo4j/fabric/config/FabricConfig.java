/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;

public class FabricConfig
{
    private final boolean enabled;
    private final Database database;
    private final List<SocketAddress> fabricServers;
    private final long routingTtl;
    private final Duration transactionTimeout;
    private final RemoteGraphDriver remoteGraphDriver;
    private final DataStream dataStream;

    public FabricConfig(
            boolean enabled,
            Database database,
            List<SocketAddress> fabricServers,
            long routingTtl,
            Duration transactionTimeout,
            RemoteGraphDriver remoteGraphDriver,
            DataStream dataStream )
    {
        this.enabled = enabled;
        this.database = database;
        this.fabricServers = fabricServers;
        this.routingTtl = routingTtl;
        this.transactionTimeout = transactionTimeout;
        this.remoteGraphDriver = remoteGraphDriver;
        this.dataStream = dataStream;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public List<SocketAddress> getFabricServers()
    {
        return fabricServers;
    }

    public long getRoutingTtl()
    {
        return routingTtl;
    }

    public Duration getTransactionTimeout()
    {
        return transactionTimeout;
    }

    public RemoteGraphDriver getRemoteGraphDriver()
    {
        return remoteGraphDriver;
    }

    public DataStream getDataStream()
    {
        return dataStream;
    }

    public Database getDatabase()
    {
        return database;
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this );
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
        FabricConfig that = (FabricConfig) o;
        return routingTtl == that.routingTtl && database.equals( that.database ) && fabricServers.equals( that.fabricServers ) &&
                transactionTimeout.equals( that.transactionTimeout ) && remoteGraphDriver.equals( that.remoteGraphDriver )
                && dataStream.equals( that.dataStream );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( database, fabricServers, routingTtl, transactionTimeout, remoteGraphDriver, dataStream );
    }

    private static String join( String... parts )
    {
        return Stream.of( parts )
                .flatMap( p -> Stream.of( ".", p ) )
                .skip( 1 )
                .collect( Collectors.joining() );
    }

    public static FabricConfig from( Config config )
    {
        var database = parseDatabase( config );

        if ( database.isEmpty() )
        {
            return new FabricConfig( false, null, null, -1, null, null, null );
        }

        var serverAddresses = config.get( FabricSettings.fabricServersSetting );
        var routingTtl = config.get( FabricSettings.routingTtlSetting );
        var transactionTimeout = config.get( GraphDatabaseSettings.transaction_timeout );
        var driverIdleTimeout = config.get( FabricSettings.driverIdleTimeout );
        var driverIdleCheckInterval = config.get( FabricSettings.driverIdleCheckInterval );

        var remoteGraphDriver = new RemoteGraphDriver( driverIdleTimeout, driverIdleCheckInterval );

        var bufferLowWatermark = config.get( FabricSettings.bufferLowWatermarkSetting );
        var bufferSize = config.get( FabricSettings.bufferSizeSetting );
        var syncBatchSize = config.get( FabricSettings.syncBatchSizeSetting );

        var dataStream = new DataStream( bufferLowWatermark, bufferSize, syncBatchSize );

        return new FabricConfig( true, database.get(), serverAddresses, routingTtl, transactionTimeout, remoteGraphDriver, dataStream );
    }

    private static Optional<Database> parseDatabase( Config config )
    {
        var databaseName = config.get( FabricSettings.databaseName );

        if ( databaseName == null )
        {
            return Optional.empty();
        }

        var graphSettings = config.getGroups( FabricSettings.GraphSetting.class ).entrySet().stream().map( entry ->
        {
            var graphId = parseGraphId( entry.getKey() );
            var graphSetting = entry.getValue();
            return new Graph( graphId, config.get( graphSetting.uri ), config.get( graphSetting.database ), config.get( graphSetting.name ) );
        } ).collect( Collectors.toSet() );

        return Optional.of( new Database( databaseName, graphSettings ) );
    }

    private static int parseGraphId( String graphKey )
    {
        try
        {
            return Integer.parseInt( graphKey );
        }
        catch ( NumberFormatException e )
        {
            throw new IllegalArgumentException( "Graph key must be a number, found: " + graphKey );
        }
    }

    public static class Database
    {
        private final String name;
        private final Set<Graph> graphs;

        public Database( String name, Set<Graph> graphs )
        {
            this.name = name;
            this.graphs = graphs;
        }

        public String getName()
        {
            return name;
        }

        public Set<Graph> getGraphs()
        {
            return graphs;
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString( this );
        }

        @Override
        public boolean equals( Object that )
        {
            return EqualsBuilder.reflectionEquals( this, that );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    public static class Graph
    {
        private final long id;
        private final URI uri;
        private final String database;
        private final String name;

        public Graph( long id, URI uri, String database, String name )
        {
            if ( uri == null )
            {
                throw new IllegalArgumentException( "Remote graph URI must be provided" );
            }

            this.id = id;
            this.uri = uri;
            this.database = database;
            this.name = name;
        }

        public long getId()
        {
            return id;
        }

        public URI getUri()
        {
            return uri;
        }

        public String getDatabase()
        {
            return database;
        }

        public String getName()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return String.format( "graph %s at %s named %s", id, uri, name );
        }

        @Override
        public boolean equals( Object that )
        {
            return EqualsBuilder.reflectionEquals( this, that );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    public static class RemoteGraphDriver
    {
        private final Duration idleTimeout;
        private final Duration driverIdleCheckInterval;

        public RemoteGraphDriver( Duration idleTimeout, Duration driverTimoutCheckInterval )
        {
            this.idleTimeout = idleTimeout;
            this.driverIdleCheckInterval = driverTimoutCheckInterval;
        }

        public Duration getIdleTimeout()
        {
            return idleTimeout;
        }

        public Duration getDriverIdleCheckInterval()
        {
            return driverIdleCheckInterval;
        }
    }

    public static class DataStream
    {
        private final int bufferLowWatermark;
        private final int bufferSize;
        private final int syncBatchSize;

        public DataStream( int bufferLowWatermark, int bufferSize, int syncBatchSize )
        {
            this.bufferLowWatermark = bufferLowWatermark;
            this.bufferSize = bufferSize;
            this.syncBatchSize = syncBatchSize;
        }

        public int getBufferLowWatermark()
        {
            return bufferLowWatermark;
        }

        public int getBufferSize()
        {
            return bufferSize;
        }

        public int getSyncBatchSize()
        {
            return syncBatchSize;
        }
    }
}
