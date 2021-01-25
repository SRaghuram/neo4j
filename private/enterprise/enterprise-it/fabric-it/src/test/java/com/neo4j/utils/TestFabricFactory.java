/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import com.neo4j.TestServer;
import com.neo4j.harness.EnterpriseNeo4jBuilders;
import org.eclipse.collections.impl.block.factory.Comparators;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;

public class TestFabricFactory
{
    private String fabricDatabaseName;
    private final List<ShardConfig> shardConfigs = new ArrayList<>();
    private final Map<String,String> settings = new HashMap<>();
    private final Map<String,String> additionalShardSettings = new HashMap<>();
    private final List<Class<?>> funcAndProcClasses = new ArrayList<>();
    private final List<Class<?>> shardFuncAndProcClasses = new ArrayList<>();
    private final List<Object> mocks = new ArrayList<>();
    private LogService logService;
    private QueryExecutionMonitor queryExecutionMonitor;
    private Path homeDir;

    public TestFabricFactory withFabricDatabase( String fabricDatabaseName )
    {
        this.fabricDatabaseName = fabricDatabaseName;
        return this;
    }

    public TestFabricFactory withShards( int shardCount )
    {
        IntStream.range( 0, shardCount ).forEach( i -> shardConfigs.add( new ShardConfig( null ) ) );
        return this;
    }

    public TestFabricFactory withShards( String... shardNames )
    {
        Arrays.stream( shardNames ).forEach( shardName -> shardConfigs.add( new ShardConfig( shardName ) ) );
        return this;
    }

    public TestFabricFactory withShards( ShardConfig... shards )
    {
        this.shardConfigs.addAll( Arrays.asList( shards ) );
        return this;
    }

    public TestFabricFactory registerFuncOrProc( Class<?> funcAndProcClass )
    {
        funcAndProcClasses.add( funcAndProcClass );
        return this;
    }

    public TestFabricFactory registerShardFuncOrProc( Class<?> funcAndProcClass )
    {
        shardFuncAndProcClasses.add( funcAndProcClass );
        return this;
    }

    public TestFabricFactory withAdditionalSettings( Map<String,String> additionalSettings )
    {
        settings.putAll( additionalSettings );
        return this;
    }

    public TestFabricFactory withAdditionalShardSettings( Map<String,String> additionalShardSettings )
    {
        this.additionalShardSettings.putAll( additionalShardSettings );
        return this;
    }

    public TestFabricFactory addMocks( Object... mocks )
    {
        this.mocks.addAll( Arrays.asList( mocks ) );
        return this;
    }

    public TestFabricFactory withLogService( LogService logService )
    {
        this.logService = logService;
        return this;
    }

    public TestFabricFactory withQueryExecutionMonitor( QueryExecutionMonitor queryExecutionMonitor )
    {
        this.queryExecutionMonitor = queryExecutionMonitor;
        return this;
    }

    public TestFabricFactory withHomeDir( Path homeDir )
    {
        this.homeDir = homeDir;
        return this;
    }

    public TestFabric build()
    {
        settings.put( "fabric.driver.connection.encrypted", "false" );
        settings.put( "dbms.connector.bolt.listen_address", "0.0.0.0:0" );
        settings.put( "dbms.connector.bolt.enabled", "true" );

        var shardBuilders = IntStream.range( 0, shardConfigs.size() )
                                     .mapToObj( i ->
                                     {
                                         var shardConfig = shardConfigs.get( i );

                                         Neo4jBuilder shardBuilder;

                                         if ( shardConfig.enterprise )
                                         {
                                             shardBuilder = EnterpriseNeo4jBuilders.newInProcessBuilder();
                                         }
                                         else
                                         {
                                             shardBuilder = Neo4jBuilders.newInProcessBuilder();
                                         }

                                         shardFuncAndProcClasses.forEach(
                                                 funcAndProcClass -> shardBuilder.withFunction( funcAndProcClass ).withProcedure( funcAndProcClass ) );

                                         var config = Config.newBuilder()
                                                            .setRaw( additionalShardSettings )
                                                            .build();

                                         config.getValues().forEach( ( key, value ) ->
                                         {
                                             if ( config.isExplicitlySet( key ) )
                                             {
                                                 shardBuilder.withConfig( key, value );
                                             }
                                         } );

                                         return new ShardBuilder( shardBuilder, i, shardConfig );
                                     } ).collect( Collectors.toList() );

        var shards = shardBuilders.parallelStream()
                                  .map( shardBuilder ->
                                  {
                                      var shardDbms = shardBuilder.shardBuilder.build();

                                      return new StartedShard( shardDbms, shardBuilder.shardId, shardBuilder.shardConfig );
                                  } )
                                  .peek( startedShard ->
                                  {
                                      settings.put( "fabric.graph." + startedShard.shardId + ".uri", startedShard.dbms.boltURI().toString() );

                                      var shardConfig = startedShard.shardConfig;
                                      if ( shardConfig.name != null )
                                      {
                                          settings.put( "fabric.graph." + startedShard.shardId + ".name", shardConfig.name );
                                      }

                                      if ( shardConfig.databaseName != null )
                                      {
                                          settings.put( "fabric.graph." + startedShard.shardId + ".database", shardConfig.databaseName );
                                      }
                                  } )
                                  .collect( Collectors.toList() );

        if ( fabricDatabaseName != null )
        {
            settings.put( "fabric.database.name", fabricDatabaseName );
        }

        var config = Config.newBuilder()
                           .setRaw( settings )
                           .build();
        TestServer testServer;
        if ( homeDir == null )
        {
            testServer = new TestServer( config );
        }
        else
        {
            testServer = new TestServer( config, homeDir );
        }

        testServer.addMocks( mocks );

        if ( logService != null )
        {
            testServer.setLogService( logService );
        }

        testServer.start();

        var globalProceduresRegistry = testServer.getDependencies().resolveDependency( GlobalProcedures.class );
        funcAndProcClasses.forEach( funcAndProcClass ->
        {
            try
            {
                globalProceduresRegistry.registerProcedure( funcAndProcClass );
                globalProceduresRegistry.registerFunction( funcAndProcClass );
            }
            catch ( KernelException e )
            {
                throw new IllegalArgumentException( e );
            }
        } );

        if ( queryExecutionMonitor != null )
        {
            testServer.getDependencies().resolveDependency( Monitors.class )
                      .addMonitorListener( queryExecutionMonitor );
        }

        var shardDbmsList = shards.stream()
                                  .sorted( Comparators.byIntFunction( shard -> shard.shardId ) )
                                  .map( shard -> shard.dbms )
                                  .collect( Collectors.toList() );
        return new TestFabric( testServer, shardDbmsList );
    }

    private static class StartedShard
    {
        private final Neo4j dbms;
        private final int shardId;
        private final ShardConfig shardConfig;

        StartedShard( Neo4j dbms, int shardId, ShardConfig shardConfig )
        {
            this.dbms = dbms;
            this.shardId = shardId;
            this.shardConfig = shardConfig;
        }
    }

    private static class ShardBuilder
    {

        private final Neo4jBuilder shardBuilder;
        private final int shardId;
        private final ShardConfig shardConfig;

        ShardBuilder( Neo4jBuilder shardBuilder, int shardId, ShardConfig shardConfig )
        {
            this.shardBuilder = shardBuilder;
            this.shardId = shardId;
            this.shardConfig = shardConfig;
        }
    }

    public static class ShardConfig
    {
        private final String name;
        private final String databaseName;
        private final boolean enterprise;

        ShardConfig( String name )
        {
            this( name, null, true );
        }

        ShardConfig( String name, String databaseName, boolean enterprise )
        {
            this.name = name;
            this.databaseName = databaseName;
            this.enterprise = enterprise;
        }
    }
}
