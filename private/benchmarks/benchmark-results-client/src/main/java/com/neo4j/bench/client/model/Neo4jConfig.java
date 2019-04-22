/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.configuration.ExternalSettings;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.tx_state_memory_allocation;

public class Neo4jConfig
{
    private static final String BOLT_ADDRESS_SETTING = "dbms.connector.bolt.listen_address";

    public static Neo4jConfig withDefaults()
    {
        return empty()
                .withSetting( UdcSettings.udc_enabled, "false" )
                .withSetting( OnlineBackupSettings.online_backup_enabled, "false" );
    }

    public static Neo4jConfig empty()
    {
        return new Neo4jConfig();
    }

    public static Neo4jConfig fromFile( File neo4jConfigFile )
    {
        return fromFile( null == neo4jConfigFile ? null : neo4jConfigFile.toPath() );
    }

    public static Neo4jConfig fromFile( Path neo4jConfigFile )
    {
        Neo4jConfig neo4jConfig = Neo4jConfig.empty();
        if ( null == neo4jConfigFile )
        {
            return neo4jConfig;
        }
        try
        {
            PropertiesConfiguration config = new PropertiesConfiguration( neo4jConfigFile.toFile() );
            Iterator<String> keys = config.getKeys();
            while ( keys.hasNext() )
            {
                String settingName = keys.next();
                if ( settingName.startsWith( ExternalSettings.additionalJvm.name() ) )
                {
                    for ( Object settingValue : config.getList( settingName ) )
                    {
                        neo4jConfig = neo4jConfig.addJvmArg( (String) settingValue );
                    }
                }
                else
                {
                    String settingValue = config.getString( settingName );
                    neo4jConfig = neo4jConfig.withStringSetting( settingName, settingValue );
                }
            }
            return neo4jConfig;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error loading neo4j configuration from: " + neo4jConfigFile.toAbsolutePath(), e );
        }
    }

    public static Neo4jConfig fromJson( String json )
    {
        return JsonUtil.deserializeJson( json, Neo4jConfig.class );
    }

    private final Map<String,String> config;
    private final List<String> jvmArgs;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    private Neo4jConfig()
    {
        this( new HashMap<>(), new ArrayList<>() );
    }

    public Neo4jConfig( Map<String,String> config )
    {
        this( requireNonNull( config ), new ArrayList<>() );
    }

    private Neo4jConfig( Map<String,String> config, List<String> jvmArgs )
    {
        this.config = requireNonNull( config );
        this.jvmArgs = requireNonNull( jvmArgs );
    }

    public Map<String,String> toMap()
    {
        return unmodifiableMap( config );
    }

    public List<String> getJvmArgs()
    {
        return Collections.unmodifiableList( jvmArgs );
    }

    public Neo4jConfig addJvmArgs( List<String> additionalJvmArgs )
    {
        Neo4jConfig newNeo4jConig = new Neo4jConfig( new HashMap<>( config ), new ArrayList<>( jvmArgs ) );
        for ( String jvmArg : additionalJvmArgs )
        {
            newNeo4jConig = newNeo4jConig.addJvmArg( jvmArg );
        }
        return newNeo4jConig;
    }

    public Neo4jConfig addJvmArg( String additionalJvmArg )
    {
        List<String> newJvmArgs = new ArrayList<>( jvmArgs );
        if ( !newJvmArgs.contains( additionalJvmArg ) )
        {
            newJvmArgs.add( additionalJvmArg );
        }
        return new Neo4jConfig( new HashMap<>( config ), newJvmArgs );
    }

    public Neo4jConfig setJvmArgs( List<String> newJvmArgs )
    {
        return new Neo4jConfig( new HashMap<>( config ), newJvmArgs );
    }

    public Neo4jConfig withSetting( Setting setting, String value )
    {
        return withStringSetting( setting.name(), value );
    }

    public Neo4jConfig removeSetting( Setting setting )
    {
        HashMap<String,String> newConfig = new HashMap<>( config );
        newConfig.remove( setting.name() );
        return new Neo4jConfig( newConfig, new ArrayList<>( jvmArgs ) );
    }

    public Neo4jConfig setDense( boolean isDense )
    {
        String denseNodeThreshold = isDense
                                    // dense node threshold set to min --> all nodes are dense
                                    ? "1"
                                    // dense node threshold set to max --> no nodes are dense
                                    : Integer.toString( Integer.MAX_VALUE );

        return withSetting( dense_node_threshold, denseNodeThreshold );
    }

    public Neo4jConfig setTransactionMemory( String setting )
    {
        String translatedValue = setting.equals( "on_heap" )
                                 ? GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP.name()
                                 : GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP.name();
        return withSetting( tx_state_memory_allocation, translatedValue );
    }

    public Neo4jConfig setBoltUri( String boltUri )
    {
        return withStringSetting( BOLT_ADDRESS_SETTING, boltUri );
    }

    private Neo4jConfig withStringSetting( String setting, String value )
    {
        HashMap<String,String> newConfig = new HashMap<>( config );
        newConfig.put( setting, value );
        return new Neo4jConfig( newConfig, new ArrayList<>( jvmArgs ) );
    }

    public Neo4jConfig mergeWith( Neo4jConfig otherNeo4jConfig )
    {
        Neo4jConfig newNeo4jConfig = new Neo4jConfig( new HashMap<>( config ), new ArrayList<>( jvmArgs ) );
        newNeo4jConfig.config.putAll( otherNeo4jConfig.config );
        return newNeo4jConfig.addJvmArgs( otherNeo4jConfig.jvmArgs );
    }

    public void writeToFile( Path file )
    {
        List<String> lines = new ArrayList<>();
        config.forEach( ( key, value ) -> lines.add( key + "=" + value ) );
        jvmArgs.forEach( jvmArg -> lines.add( ExternalSettings.additionalJvm.name() + "=" + jvmArg ) );
        String contents = String.join( "\n", lines ) + "\n";
        BenchmarkUtil.stringToFile( contents, file );
    }

    public String toJson()
    {
        return JsonUtil.serializeJson( this );
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
        Neo4jConfig that = (Neo4jConfig) o;
        return Objects.equals( config, that.config );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( config );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + ":\n" +
               "JVM Args: " + jvmArgs + "\n" +
               BenchmarkUtil.prettyPrint( config );
    }
}
