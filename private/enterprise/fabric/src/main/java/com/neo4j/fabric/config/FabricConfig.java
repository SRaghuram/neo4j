/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.eclipse.collections.api.multimap.set.MutableSetMultimap;
import org.eclipse.collections.impl.factory.Multimaps;

import java.net.URI;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Level;

public class FabricConfig
{
    private final boolean enabled;
    private final Database database;
    private final List<SocketAddress> fabricServers;
    private final Duration routingTtl;
    private final Duration transactionTimeout;
    private final GlobalDriverConfig globalDriverConfig;
    private final DataStream dataStream;

    public FabricConfig(
            boolean enabled,
            Database database,
            List<SocketAddress> fabricServers,
            Duration routingTtl,
            Duration transactionTimeout,
            GlobalDriverConfig globalDriverConfig,
            DataStream dataStream )
    {
        this.enabled = enabled;
        this.database = database;
        this.fabricServers = fabricServers;
        this.routingTtl = routingTtl;
        this.transactionTimeout = transactionTimeout;
        this.globalDriverConfig = globalDriverConfig;
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

    public Duration getRoutingTtl()
    {
        return routingTtl;
    }

    public Duration getTransactionTimeout()
    {
        return transactionTimeout;
    }

    public GlobalDriverConfig getGlobalDriverConfig()
    {
        return globalDriverConfig;
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
                transactionTimeout.equals( that.transactionTimeout ) && globalDriverConfig.equals( that.globalDriverConfig )
                && dataStream.equals( that.dataStream );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( database, fabricServers, routingTtl, transactionTimeout, globalDriverConfig, dataStream );
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
            return new FabricConfig( false, null, null, null, null, null, null );
        }

        var serverAddresses = config.get( FabricSettings.fabricServersSetting );
        var routingTtl = config.get( FabricSettings.routingTtlSetting );
        var transactionTimeout = config.get( GraphDatabaseSettings.transaction_timeout );
        var driverIdleTimeout = config.get( FabricSettings.driverIdleTimeout );
        var driverIdleCheckInterval = config.get( FabricSettings.driverIdleCheckInterval );
        var driverEventLoopCount = config.get( FabricSettings.driverEventLoopCount );

        var driverConfig = new DriverConfig( config.get( FabricSettings.driverLoggingLevel ), config.get( FabricSettings.driverLogLeakedSessions ),
                config.get( FabricSettings.driverMaxConnectionPoolSize ), config.get( FabricSettings.driverIdleTimeBeforeConnectionTest ),
                config.get( FabricSettings.driverMaxConnectionLifetime ), config.get( FabricSettings.driverConnectionAcquisitionTimeout ),
                config.get( FabricSettings.driverEncrypted ), config.get( FabricSettings.driverTrustStrategy ),
                config.get( FabricSettings.driverConnectTimeout ),
                config.get( FabricSettings.driverApi ) );

        var remoteGraphDriver = new GlobalDriverConfig( driverIdleTimeout, driverIdleCheckInterval, driverEventLoopCount, driverConfig );

        var bufferLowWatermark = config.get( FabricSettings.bufferLowWatermarkSetting );
        var bufferSize = config.get( FabricSettings.bufferSizeSetting );
        var syncBatchSize = config.get( FabricSettings.syncBatchSizeSetting );

        var dataStream = new DataStream( bufferLowWatermark, bufferSize, syncBatchSize );

        return new FabricConfig( true, database.get(), serverAddresses, routingTtl, transactionTimeout, remoteGraphDriver, dataStream );
    }

    private static Optional<Database> parseDatabase( Config config )
    {
        var databaseNameRaw = config.get( FabricSettings.databaseName );

        if ( databaseNameRaw == null )
        {
            return Optional.empty();
        }

        var databaseName = new NormalizedDatabaseName( databaseNameRaw );
        var graphSettings = config.getGroups( FabricSettings.GraphSetting.class ).entrySet().stream().map( entry ->
        {
            var graphId = parseGraphId( entry.getKey() );
            var graphSetting = entry.getValue();
            var driverConfig = new DriverConfig( config.get( graphSetting.driverLoggingLevel ), config.get( graphSetting.driverLogLeakedSessions ),
                    config.get( graphSetting.driverMaxConnectionPoolSize ), config.get( graphSetting.driverIdleTimeBeforeConnectionTest ),
                    config.get( graphSetting.driverMaxConnectionLifetime ), config.get( graphSetting.driverConnectionAcquisitionTimeout ),
                    config.get( graphSetting.driverEncrypted ), config.get( graphSetting.driverTrustStrategy ), config.get( graphSetting.driverConnectTimeout ),
                    config.get( graphSetting.driverApi ) );

            var remoteUri = new RemoteUri( config.get( graphSetting.uris ) );
            return new Graph( graphId, remoteUri, config.get( graphSetting.database ), config.get( graphSetting.name ), driverConfig );
        } ).collect( Collectors.toSet() );

        validateGraphNames( graphSettings );

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

    private static void validateGraphNames( Set<Graph> graphSettings )
    {
        MutableSetMultimap<String,Graph> graphsByName = Multimaps.mutable.set.empty();
        graphSettings.stream()
                .filter( g -> g.name != null )
                .forEach( g -> graphsByName.put( new NormalizedDatabaseName( g.name ).name(), g ) );

        graphsByName.multiValuesView().forEach( graphs ->
        {
            if ( graphs.size() > 1 )
            {
                var sortedGraphs = graphs.toSortedList( Comparator.comparingLong( Graph::getId ) );
                var names = sortedGraphs.collect( Graph::getName ).distinct().makeString( ", " );
                var ids = sortedGraphs.collect( Graph::getId ).distinct().makeString( ", " );
                throw new IllegalArgumentException( "Graphs with ids: " + ids + ", have conflicting names: " + names );
            }
        } );
    }

    public static class Database
    {
        private final NormalizedDatabaseName name;
        private final Set<Graph> graphs;

        public Database( NormalizedDatabaseName name, Set<Graph> graphs )
        {
            this.name = name;
            this.graphs = graphs;
        }

        public NormalizedDatabaseName getName()
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
        private final RemoteUri uri;
        private final String database;
        private final String name;
        private final DriverConfig driverConfig;

        public Graph( long id, RemoteUri uri, String database, String name, DriverConfig driverConfig )
        {
            if ( uri == null )
            {
                throw new IllegalArgumentException( "Remote graph URI must be provided" );
            }

            this.id = id;
            this.uri = uri;
            this.database = database;
            this.name = name;
            this.driverConfig = driverConfig;
        }

        public long getId()
        {
            return id;
        }

        public RemoteUri getUri()
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

        public DriverConfig getDriverConfig()
        {
            return driverConfig;
        }

        @Override
        public String toString()
        {
            return String.format( "graph %s named %s", id,  name );
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

    public static class DriverConfig
    {

        private final Level loggingLevel;
        private final Boolean logLeakedSessions;
        private final Integer maxConnectionPoolSize;
        private final Duration idleTimeBeforeConnectionTest;
        private final Duration maxConnectionLifetime;
        private final Duration connectionAcquisitionTimeout;
        private final Boolean encrypted;
        private final FabricSettings.DriverTrustStrategy trustStrategy;
        private final Duration connectTimeout;
        private final FabricSettings.DriverApi driverApi;

        public DriverConfig( Level loggingLevel, Boolean logLeakedSessions, Integer maxConnectionPoolSize, Duration idleTimeBeforeConnectionTest,
                Duration maxConnectionLifetime, Duration connectionAcquisitionTimeout, Boolean encrypted, FabricSettings.DriverTrustStrategy trustStrategy,
                Duration connectTimeout, FabricSettings.DriverApi driverApi )
        {
            this.loggingLevel = loggingLevel;
            this.logLeakedSessions = logLeakedSessions;
            this.maxConnectionPoolSize = maxConnectionPoolSize;
            this.idleTimeBeforeConnectionTest = idleTimeBeforeConnectionTest;
            this.maxConnectionLifetime = maxConnectionLifetime;
            this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
            this.encrypted = encrypted;
            this.trustStrategy = trustStrategy;
            this.connectTimeout = connectTimeout;
            this.driverApi = driverApi;
        }

        public Level getLoggingLevel()
        {
            return loggingLevel;
        }

        public Boolean getLogLeakedSessions()
        {
            return logLeakedSessions;
        }

        public Integer getMaxConnectionPoolSize()
        {
            return maxConnectionPoolSize;
        }

        public Duration getIdleTimeBeforeConnectionTest()
        {
            return idleTimeBeforeConnectionTest;
        }

        public Duration getMaxConnectionLifetime()
        {
            return maxConnectionLifetime;
        }

        public Duration getConnectionAcquisitionTimeout()
        {
            return connectionAcquisitionTimeout;
        }

        public Boolean getEncrypted()
        {
            return encrypted;
        }

        public FabricSettings.DriverTrustStrategy getTrustStrategy()
        {
            return trustStrategy;
        }

        public Duration getConnectTimeout()
        {
            return connectTimeout;
        }

        public FabricSettings.DriverApi getDriverApi()
        {
            return driverApi;
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

    public static class GlobalDriverConfig
    {
        private final Duration idleTimeout;
        private final Duration driverIdleCheckInterval;
        private final int eventLoopCount;
        private final DriverConfig driverConfig;

        public GlobalDriverConfig( Duration idleTimeout, Duration driverTimoutCheckInterval, int eventLoopCount, DriverConfig driverConfig )
        {
            this.idleTimeout = idleTimeout;
            this.driverIdleCheckInterval = driverTimoutCheckInterval;
            this.eventLoopCount = eventLoopCount;
            this.driverConfig = driverConfig;
        }

        public Duration getIdleTimeout()
        {
            return idleTimeout;
        }

        public Duration getDriverIdleCheckInterval()
        {
            return driverIdleCheckInterval;
        }

        public int getEventLoopCount()
        {
            return eventLoopCount;
        }

        public DriverConfig getDriverConfig()
        {
            return driverConfig;
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

    public static class RemoteUri
    {
        private final String scheme;
        private final List<SocketAddress> addresses;
        private final String query;

        public RemoteUri( List<URI> uris )
        {
            if ( uris == null || uris.isEmpty() )
            {
                throw new IllegalArgumentException( "Remote graph URI must be provided" );
            }

            var mainUri = uris.get( 0 );

            if ( mainUri.getScheme() == null )
            {
                throw new IllegalArgumentException( "Scheme must be provided: " + uris );
            }

            scheme = mainUri.getScheme();
            query = mainUri.getQuery();

            boolean match = IntStream.range( 1, uris.size() )
                    .mapToObj( uris::get )
                    .allMatch( uri -> Objects.equals( scheme, uri.getScheme() ) && Objects.equals( query, uri.getQuery() ) );
            if ( !match )
            {
                throw new IllegalArgumentException( "URI mismatch: " + uris );
            }

            addresses = uris.stream()
                    .peek( uri ->
                    {
                        if ( uri.getHost() == null || uri.getPort() == -1 )
                        {
                            throw new IllegalArgumentException( "Host name and port must be provided: " + uris );
                        }
                    } )
                    .map( uri ->  new SocketAddress( uri.getHost(), uri.getPort() ))
                    .collect( Collectors.toList());
        }

        public static RemoteUri create( String uri )
        {
            return new RemoteUri( List.of( URI.create( uri ) ) );
        }

        public static RemoteUri create( URI uri )
        {
            return new RemoteUri( List.of( uri ) );
        }

        public String getScheme()
        {
            return scheme;
        }

        public List<SocketAddress> getAddresses()
        {
            return addresses;
        }

        public String getQuery()
        {
            return query;
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
            RemoteUri remoteUri = (RemoteUri) o;
            return scheme.equals( remoteUri.scheme ) && addresses.equals( remoteUri.addresses ) && Objects.equals( query, remoteUri.query );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( scheme, addresses, query );
        }
    }
}
