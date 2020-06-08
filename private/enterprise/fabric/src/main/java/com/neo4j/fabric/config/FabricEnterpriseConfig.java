/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import com.neo4j.fabric.driver.DriverConfigFactory;
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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.configuration.helpers.NormalizedGraphName;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.config.FabricSettings;
import org.neo4j.logging.Level;

import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;

public class FabricEnterpriseConfig extends FabricConfig
{
    private final Database database;
    private final Duration routingTtl;

    private final GlobalDriverConfig globalDriverConfig;

    private volatile List<SocketAddress> fabricServers;

    public FabricEnterpriseConfig(
            Database database,
            List<SocketAddress> fabricServers,
            Duration routingTtl,
            Duration transactionTimeout,
            GlobalDriverConfig globalDriverConfig,
            DataStream dataStream )
    {
        super( transactionTimeout, dataStream );
        this.database = database;
        this.fabricServers = fabricServers;
        this.routingTtl = routingTtl;
        this.globalDriverConfig = globalDriverConfig;
    }

    @Override
    public Optional<NormalizedDatabaseName> getFabricDatabaseName()
    {
        return Optional.ofNullable( database ).map( database -> database.name );
    }

    public List<SocketAddress> getFabricServers()
    {
        return fabricServers;
    }

    public void setFabricServers( List<SocketAddress> fabricServers )
    {
        this.fabricServers = fabricServers;
    }

    public Duration getRoutingTtl()
    {
        return routingTtl;
    }

    public GlobalDriverConfig getGlobalDriverConfig()
    {
        return globalDriverConfig;
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
        FabricEnterpriseConfig that = (FabricEnterpriseConfig) o;
        return routingTtl == that.routingTtl && database.equals( that.database ) && fabricServers.equals( that.fabricServers ) &&
                getTransactionTimeout().equals( that.getTransactionTimeout() ) && globalDriverConfig.equals( that.globalDriverConfig )
                && getDataStream().equals( that.getDataStream() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( database, fabricServers, routingTtl, getTransactionTimeout(), globalDriverConfig, getDataStream() );
    }

    public static FabricEnterpriseConfig from( Config config )
    {
        var database = parseDatabase( config );
        var transactionTimeout = config.get( GraphDatabaseSettings.transaction_timeout );
        var driverIdleTimeout = config.get( FabricEnterpriseSettings.driverIdleTimeout );
        var driverIdleCheckInterval = config.get( FabricEnterpriseSettings.driverIdleCheckInterval );
        var driverEventLoopCount = config.get( FabricEnterpriseSettings.driverEventLoopCount );

        var driverConfig = new DriverConfig( config.get( FabricEnterpriseSettings.driverLoggingLevel ),
                config.get( FabricEnterpriseSettings.driverLogLeakedSessions ),
                config.get( FabricEnterpriseSettings.driverMaxConnectionPoolSize ),
                config.get( FabricEnterpriseSettings.driverIdleTimeBeforeConnectionTest ),
                config.get( FabricEnterpriseSettings.driverMaxConnectionLifetime ),
                config.get( FabricEnterpriseSettings.driverConnectionAcquisitionTimeout ),
                config.get( FabricEnterpriseSettings.driverConnectTimeout ),
                config.get( FabricEnterpriseSettings.driverApi ) );

        var remoteGraphDriver = new GlobalDriverConfig( driverIdleTimeout, driverIdleCheckInterval, driverEventLoopCount, driverConfig );

        var bufferLowWatermark = config.get( FabricEnterpriseSettings.bufferLowWatermarkSetting );
        var bufferSize = config.get( FabricEnterpriseSettings.bufferSizeSetting );
        var syncBatchSize = config.get( FabricSettings.batchSizeSetting );
        var concurrency = config.get( FabricEnterpriseSettings.concurrency );
        if ( concurrency == null )
        {
            concurrency = database.map( db -> db.graphs.size() ).orElse( 1 );
        }

        var dataStream = new DataStream( bufferLowWatermark, bufferSize, syncBatchSize, concurrency );

        if ( database.isPresent() )
        {
            var serverAddresses = config.get( FabricEnterpriseSettings.fabricServersSetting );
            var routingTtl = config.get( FabricEnterpriseSettings.routingTtlSetting );
            var fabricConfig = new FabricEnterpriseConfig( database.get(), serverAddresses, routingTtl, transactionTimeout, remoteGraphDriver, dataStream );
            config.addListener( FabricEnterpriseSettings.fabricServersSetting, ( oldValue, newValue ) -> fabricConfig.setFabricServers( newValue ) );
            return fabricConfig;
        }
        else
        {
            return new FabricEnterpriseConfig( null, List.of(), null, transactionTimeout, remoteGraphDriver, dataStream );
        }
    }

    private static Optional<Database> parseDatabase( Config config )
    {
        var databaseNameRaw = config.get( FabricEnterpriseSettings.databaseName );

        if ( databaseNameRaw == null )
        {
            return Optional.empty();
        }

        var databaseName = new NormalizedDatabaseName( databaseNameRaw );
        var graphSettings = config.getGroups( FabricEnterpriseSettings.GraphSetting.class ).entrySet().stream().map( entry ->
        {
            var graphId = parseGraphId( entry.getKey() );
            var graphSetting = entry.getValue();
            var driverConfig = new GraphDriverConfig( config.get( graphSetting.driverLoggingLevel ), config.get( graphSetting.driverLogLeakedSessions ),
                    config.get( graphSetting.driverMaxConnectionPoolSize ), config.get( graphSetting.driverIdleTimeBeforeConnectionTest ),
                    config.get( graphSetting.driverMaxConnectionLifetime ), config.get( graphSetting.driverConnectionAcquisitionTimeout ),
                    config.get( graphSetting.driverConnectTimeout ), config.get( graphSetting.driverApi ), config.get( graphSetting.sslEnabled ) );

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
        MutableSetMultimap<NormalizedGraphName,Graph> graphsByName = Multimaps.mutable.set.empty();
        graphSettings.stream()
                .filter( g -> g.name != null )
                .forEach( g -> graphsByName.put( g.name, g ) );

        graphsByName.multiValuesView().forEach( graphs ->
        {
            if ( graphs.size() > 1 )
            {
                var sortedGraphs = graphs.toSortedList( Comparator.comparingLong( Graph::getId ) );
                var ids = sortedGraphs.collect( Graph::getId ).distinct().makeString( ", " );
                throw new IllegalArgumentException( "Graphs with ids: " + ids + ", have conflicting names" );
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
            return reflectionEquals( this, that );
        }

        @Override
        public int hashCode()
        {
            return reflectionHashCode( this );
        }
    }

    public static class Graph
    {
        private final long id;
        private final RemoteUri uri;
        private final String database;
        private final NormalizedGraphName name;
        private final GraphDriverConfig driverConfig;

        public Graph( long id, RemoteUri uri, String database, NormalizedGraphName name, GraphDriverConfig driverConfig )
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

        public NormalizedGraphName getName()
        {
            return name;
        }

        public GraphDriverConfig getDriverConfig()
        {
            return driverConfig;
        }

        @Override
        public String toString()
        {
            return String.format( "graph %s named %s", id, name );
        }

        @Override
        public boolean equals( Object that )
        {
            return reflectionEquals( this, that );
        }

        @Override
        public int hashCode()
        {
            return reflectionHashCode( this );
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
        private final Duration connectTimeout;
        private final DriverConfigFactory.DriverApi driverApi;

        public DriverConfig( Level loggingLevel, Boolean logLeakedSessions, Integer maxConnectionPoolSize, Duration idleTimeBeforeConnectionTest,
                Duration maxConnectionLifetime, Duration connectionAcquisitionTimeout, Duration connectTimeout,
                DriverConfigFactory.DriverApi driverApi )
        {
            this.loggingLevel = loggingLevel;
            this.logLeakedSessions = logLeakedSessions;
            this.maxConnectionPoolSize = maxConnectionPoolSize;
            this.idleTimeBeforeConnectionTest = idleTimeBeforeConnectionTest;
            this.maxConnectionLifetime = maxConnectionLifetime;
            this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
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

        public Duration getConnectTimeout()
        {
            return connectTimeout;
        }

        public DriverConfigFactory.DriverApi getDriverApi()
        {
            return driverApi;
        }

        @Override
        public boolean equals( Object that )
        {
            return reflectionEquals( this, that );
        }

        @Override
        public int hashCode()
        {
            return reflectionHashCode( this );
        }
    }

    public static class GraphDriverConfig extends DriverConfig
    {
        private final boolean sslEnabled;

        public GraphDriverConfig( Level loggingLevel, Boolean logLeakedSessions, Integer maxConnectionPoolSize, Duration idleTimeBeforeConnectionTest,
                Duration maxConnectionLifetime, Duration connectionAcquisitionTimeout, Duration connectTimeout,
                DriverConfigFactory.DriverApi driverApi, boolean sslEnabled )
        {
            super( loggingLevel, logLeakedSessions, maxConnectionPoolSize, idleTimeBeforeConnectionTest, maxConnectionLifetime, connectionAcquisitionTimeout,
                    connectTimeout, driverApi );

            this.sslEnabled = sslEnabled;
        }

        public boolean isSslEnabled()
        {
            return sslEnabled;
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

            boolean sameScheme = uris.stream()
                    .skip( 1 )
                    .allMatch( uri -> Objects.equals( scheme, uri.getScheme() ) );
            if ( !sameScheme )
            {
                throw new IllegalArgumentException( "URIs must have the same scheme: " + uris );
            }

            boolean sameQuery = uris.stream()
                    .skip( 1 )
                    .allMatch( uri -> Objects.equals( query, uri.getQuery() ) );
            if ( !sameQuery )
            {
                throw new IllegalArgumentException( "URIs must have the same query: " + uris );
            }

            addresses = uris.stream()
                    .peek( uri ->
                    {
                        if ( uri.getHost() == null || uri.getPort() == -1 )
                        {
                            throw new IllegalArgumentException( "Host name and port must be provided: " + uris );
                        }
                    } )
                    // IPv6 address will be in [] which is invalid from socket perspective. SocketAddressParser can deal with it
                    .map( uri -> SocketAddressParser.socketAddress( uri.getHost() + ":" + uri.getPort(), SocketAddress::new ) )
                    .collect( Collectors.toList() );
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
