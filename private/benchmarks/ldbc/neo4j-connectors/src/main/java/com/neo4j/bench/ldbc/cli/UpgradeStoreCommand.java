/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.ldbc.driver.DbException;
import com.ldbc.driver.util.FileUtils;
import com.ldbc.driver.util.MapUtils;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcIndexer;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

import static com.neo4j.bench.ldbc.cli.RunCommand.discoverSchema;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@Command(
        name = "upgrade-store",
        description = "Used to simplify the task of upgrading stores to new version/store format" )
public class UpgradeStoreCommand implements Runnable
{
    public static final String CMD_ORIGINAL_DB = "--original-db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ORIGINAL_DB},
            description = "Neo4j database that needs to be upgraded. E.g., db_sf001_p006_regular_utc_36ce/graph.db/",
            title = "Original Neo4j DB ",
            required = true )
    private File originalDbDir;

    public static final String CMD_UPGRADED_DB = "--upgraded-db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_UPGRADED_DB},
            description = "Neo4j database to copy into working directory. E.g., db_sf001_p006_regular_utc_40ce/graph.db/",
            title = "Upgraded Neo4j database",
            required = true )
    private File upgradedDbDir;

    public static final String CMD_RECREATE_INDEXES = "--recreate-indexes";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RECREATE_INDEXES},
            description = "Forces indexes to be recreated",
            title = "Forces indexes to be recreated",
            required = false )
    private boolean recreateIndexes;

    public static final String CMD_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CONFIG},
            description = "Neo4j configuration file",
            title = "Neo4j Config",
            required = false )
    private File neo4jConfigFile;

    @Override
    public void run()
    {
        String recordFormat = recordFormatOrFail( neo4jConfigFile );

        System.out.println( "Store upgrade..." );
        Store.assertDirectoryIsNeoStore( originalDbDir.toPath() );
        Store originalStore = Store.createFrom( originalDbDir.toPath() );
        Store upgradedStore = originalStore.makeCopyAt( upgradedDbDir.toPath() );
        // Note, index & transaction log removal may break if store directory structure changes between previous and current versions
        upgradedStore.removeIndexDir();
        upgradedStore.removeTxLogs();

        long storeSizeInMb = storeSizeInMb( upgradedStore );
        File neo4jConfigFile = makeConfigFile( storeSizeInMb, recordFormat );
        if ( recreateIndexes )
        {
            try
            {
                System.out.println( "Starting store and recreating Indexes..." );
                Neo4jSchema neo4jSchema = discoverSchema( upgradedStore.topLevelDirectory().toFile(), neo4jConfigFile, null );
                DatabaseManagementService managementService = Neo4jDb.newDb( upgradedStore.graphDbDirectory().toFile(), neo4jConfigFile );
                GraphDatabaseService db = managementService.database( upgradedStore.graphDbDirectory().getFileName().toString() );
                LdbcIndexer ldbcIndexer = new LdbcIndexer( neo4jSchema, true, false, true );
                ldbcIndexer.createTransactional( db );
                System.out.println( "Shutting down store..." );
                managementService.shutdown();
            }
            catch ( DbException e )
            {
                throw new RuntimeException( "Error upgrading store", e );
            }
        }
        else
        {
            System.out.println( "Starting store..." );
            DatabaseManagementService managementService = Neo4jDb.newDb( upgradedStore.graphDbDirectory().toFile(), neo4jConfigFile );
            System.out.println( "Shutting down store..." );
            managementService.shutdown();
        }
        System.out.println( "Store upgrade complete" );
    }

    private static String recordFormatOrFail( File neo4jConfigFile )
    {
        try
        {
            if ( null == neo4jConfigFile )
            {
                throw new RuntimeException( "No Neo4j config file provided" );
            }
            Map<String,String> neo4jConfig = MapUtils.loadPropertiesToMap( neo4jConfigFile );
            if ( !neo4jConfig.containsKey( record_format.name() ) )
            {
                throw new RuntimeException( format( "Could not find '%s' in Neo4j config", record_format.name() ) );
            }
            else
            {
                return neo4jConfig.get( record_format.name() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Error loading Neo4j Config" );
        }
    }

    private static File makeConfigFile( long storeSizeInMb, String recordFormat )
    {
        try
        {
            File neo4jConfigFile = new File( "generated_neo4j.conf" );
            FileUtils.forceRecreateFile( neo4jConfigFile );
            Map<String,String> neo4jConfigMap = new HashMap<>();
            neo4jConfigMap.put( record_format.name(), recordFormat );
            neo4jConfigMap.put( allow_upgrade.name(), "true" );
            neo4jConfigMap.put( pagecache_memory.name(), storeSizeInMb + "m" );
            Properties neo4jConfigProperties = MapUtils.mapToProperties( neo4jConfigMap );
            try ( FileOutputStream stream = new FileOutputStream( neo4jConfigFile ) )
            {
                neo4jConfigProperties.store( stream, null );
            }
            return neo4jConfigFile;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Unable to create Neo4j config file", e );
        }
    }

    private static long storeSizeInMb( Store store )
    {
        return store.bytes() / 1024 / 1024;
    }
}
