/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common;

import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.neo4j.configuration.ExternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

public class Neo4jConfigBuilder
{

    private static final String BOLT_ADDRESS_SETTING = "dbms.connector.bolt.listen_address";

    public static Neo4jConfigBuilder withDefaults()
    {
        return empty()
                .withSetting( OnlineBackupSettings.online_backup_enabled, FALSE )
                .withSetting( GraphDatabaseSettings.fail_on_missing_files, FALSE )
                .withSetting( HttpConnector.enabled, FALSE )
                .withSetting( HttpConnector.enabled, FALSE )
                .withSetting( OnlineBackupSettings.online_backup_enabled, FALSE )
                .withSetting( GraphDatabaseSettings.log_queries, FALSE )
                .withSetting( ServerSettings.http_logging_enabled, FALSE )
                .withSetting( GraphDatabaseSettings.strict_config_validation, TRUE );
    }

    public static Neo4jConfigBuilder empty()
    {
        return new Neo4jConfigBuilder( Neo4jConfig.empty() );
    }

    public static Neo4jConfigBuilder fromFile( File neo4jConfigFile )
    {
        return fromFile( null == neo4jConfigFile ? null : neo4jConfigFile.toPath() );
    }

    public static Neo4jConfigBuilder fromFile( Path neo4jConfigFile )
    {
        Neo4jConfig neo4jConfig = Neo4jConfig.empty();
        if ( null == neo4jConfigFile )
        {
            return new Neo4jConfigBuilder( neo4jConfig );
        }
        try
        {
            BenchmarkUtil.assertFileExists( neo4jConfigFile );
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
                    neo4jConfig = neo4jConfig.withSetting( settingName, settingValue );
                }
            }
            return new Neo4jConfigBuilder( neo4jConfig );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error loading neo4j configuration from: " + neo4jConfigFile.toAbsolutePath(), e );
        }
    }

    public static void writeToFile( Neo4jConfig neo4jConfig, Path file )
    {
        List<String> lines = new ArrayList<>();
        neo4jConfig.toMap().forEach( ( key, value ) -> lines.add( key + "=" + value ) );
        neo4jConfig.getJvmArgs().forEach( jvmArg -> lines.add( ExternalSettings.additionalJvm.name() + "=" + jvmArg ) );
        String contents = String.join( "\n", lines ) + "\n";
        BenchmarkUtil.stringToFile( contents, file );
    }

    private Neo4jConfig neo4jConfig = Neo4jConfig.empty();

    private Neo4jConfigBuilder( Neo4jConfig neo4jConfig )
    {
        this.neo4jConfig = neo4jConfig;
    }

    public Neo4jConfigBuilder withSetting( Setting setting, String value )
    {
        neo4jConfig = neo4jConfig.withSetting( setting.name(), value );
        return this;
    }

    public Neo4jConfigBuilder removeSetting( Setting setting )
    {
        HashMap<String,String> newConfig = new HashMap<>( neo4jConfig.toMap() );
        newConfig.remove( setting.name() );
        neo4jConfig = Neo4jConfig.from( newConfig, new ArrayList<>( neo4jConfig.getJvmArgs() ) );
        return this;
    }

    public Neo4jConfigBuilder setDense( boolean isDense )
    {
        String denseNodeThreshold = isDense
                                    // dense node threshold set to min --> all nodes are dense
                                    ? "1"
                                    // dense node threshold set to max --> no nodes are dense
                                    : Integer.toString( Integer.MAX_VALUE );

        return withSetting( dense_node_threshold, denseNodeThreshold );
    }

    public Neo4jConfigBuilder setTransactionMemory( String setting )
    {
        String translatedValue;
        if ( setting.equals( "on_heap" ) )
        {
            translatedValue = GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP.name();
        }
        else if ( setting.equals( "off_heap" ) )
        {
            translatedValue = GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP.name();
        }
        else
        {
            translatedValue = GraphDatabaseSettings.tx_state_memory_allocation.defaultValue().name();
        }
        return withSetting( tx_state_memory_allocation, translatedValue );
    }

    public Neo4jConfigBuilder setBoltUri( String boltUri )
    {
        neo4jConfig = neo4jConfig.withSetting( BOLT_ADDRESS_SETTING, boltUri );
        return this;
    }

    public Neo4jConfigBuilder addJvmArgs( List<String> additionalJvmArgs )
    {
        neo4jConfig = neo4jConfig.addJvmArgs( additionalJvmArgs );
        return this;
    }

    public Neo4jConfig build()
    {
        return neo4jConfig;
    }

    public Neo4jConfigBuilder mergeWith( Neo4jConfig otherConfig )
    {
        neo4jConfig = neo4jConfig.mergeWith( otherConfig );
        return this;
    }

    public void writeToFile( Path neo4jConfigPath )
    {
        writeToFile( neo4jConfig, neo4jConfigPath );
    }
}
