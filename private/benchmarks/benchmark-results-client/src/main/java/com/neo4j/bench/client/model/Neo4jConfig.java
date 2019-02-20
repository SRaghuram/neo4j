/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.tx_state_memory_allocation;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class Neo4jConfig
{

    public static Neo4jConfig withDefaults()
    {
        return empty()
                .withSetting( UdcSettings.udc_enabled, "false" )
                .withSetting( OnlineBackupSettings.online_backup_enabled, "false" );
    }

    public static Neo4jConfig empty()
    {
        return new Neo4jConfig( emptyMap() );
    }

    public static Neo4jConfig fromFile( File neo4jConfigFile )
    {
        return fromFile( null == neo4jConfigFile ? null : neo4jConfigFile.toPath() );
    }

    public static Neo4jConfig fromFile( Path neo4jConfigFile )
    {
        return (null == neo4jConfigFile)
               ? empty()
               : new Neo4jConfig( BenchmarkUtil.propertiesPathToMap( neo4jConfigFile ) );
    }

    public static Neo4jConfig fromJson( String json )
    {
        return JsonUtil.deserializeJson( json, Neo4jConfig.class );
    }

    private final Map<String,String> config;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Neo4jConfig()
    {
        this( emptyMap() );
    }

    public Neo4jConfig( Map<String,String> config )
    {
        this.config = requireNonNull( config );
    }

    public Map<String,String> toMap()
    {
        return unmodifiableMap( config );
    }

    public Neo4jConfig withSetting( Setting setting, String value )
    {
        HashMap<String,String> newConfig = new HashMap<>( config );
        newConfig.put( setting.name(), value );
        return new Neo4jConfig( newConfig );
    }

    public Neo4jConfig removeSetting( Setting setting )
    {
        HashMap<String,String> newConfig = new HashMap<>( config );
        newConfig.remove( setting.name() );
        return new Neo4jConfig( newConfig );
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

    public Neo4jConfig mergeWith( Neo4jConfig otherNeo4jConfig )
    {
        HashMap<String,String> newConfig = new HashMap<>( config );
        newConfig.putAll( otherNeo4jConfig.config );
        return new Neo4jConfig( newConfig );
    }

    public void writeAsProperties( Path file )
    {
        BenchmarkUtil.mapToFile( config, file );
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
        return BenchmarkUtil.prettyPrint( config ) ;
    }
}
