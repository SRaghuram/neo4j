/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.workload.Workload;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.neo4j.bench.macro.execution.Neo4jDeployment.DeploymentMode;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@Command( name = "upgrade-store", description = "Upgrades a Neo4j store, including rebuilding of indexes." )
public class UpgradeStoreCommand implements Runnable
{
    private static final String CMD_ORIGINAL_DB = "--original-db";
    @Option( type = OptionType.COMMAND,
             name = {CMD_ORIGINAL_DB},
             description = "Neo4j database that needs to be upgraded. E.g. 'accesscontrol/' not 'accesscontrol/graph.db/'",
             title = "Original Neo4j DB ",
             required = true )
    private File originalDbDir;

    private static final String CMD_UPGRADED_DB = "--upgraded-db";
    @Option( type = OptionType.COMMAND,
             name = {CMD_UPGRADED_DB},
             description = "Neo4j database to copy into working directory. E.g. 'new_accesscontrol/' not 'new_accesscontrol/graph.db/'",
             title = "Upgraded Neo4j database",
             required = true )
    private File upgradedDbDir;

    private static final String CMD_WORKLOAD = "--workload";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WORKLOAD},
             description = "Path to workload configuration file",
             title = "Workload configuration",
             required = true )
    private String workloadName;

    private static final String CMD_EDITION = "--db-edition";
    @Option( type = OptionType.COMMAND,
             name = {CMD_EDITION},
             description = "Neo4j edition: COMMUNITY or ENTERPRISE",
             title = "Neo4j edition",
             required = true )
    private Edition edition;

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_CONFIG},
             title = "Neo4j configuration file",
             required = false )
    private File neo4jConfigFile;

    @Override
    public void run()
    {
        System.out.println( format( "Upgrading store for workload `%s`\n" +
                                    "Old store: `%s`\n" +
                                    "New store: `%s`",
                                    workloadName,
                                    originalDbDir.getAbsolutePath(),
                                    upgradedDbDir.getAbsolutePath() ) );

        Store.assertDirectoryIsNeoStore( originalDbDir.toPath() );
        Path workDir = Paths.get( System.getProperty( "user.dir" ) );
        try ( Store originalStore = Store.createFrom( originalDbDir.toPath() );
              Resources resources = new Resources( workDir ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, DeploymentMode.EMBEDDED );

            Path neo4jConfigPath = (null == neo4jConfigFile) ? null : neo4jConfigFile.toPath();
            if ( neo4jConfigPath != null )
            {
                BenchmarkUtil.assertFileNotEmpty( neo4jConfigPath );
            }
            else
            {
                neo4jConfigPath = Paths.get( "neo4j.conf" );
                Neo4jConfigBuilder.empty()
                                  .withSetting( allow_upgrade, TRUE )
                                  .withSetting( record_format, "high_limit" )
                                  .writeToFile( neo4jConfigPath );
            }

            System.out.println( "Checking schema..." );
            EmbeddedDatabase.verifySchema( originalStore, edition, neo4jConfigPath, workload.expectedSchema() );

            System.out.println( "Copying store\n" +
                                "From: " + originalDbDir.getAbsolutePath() + "\n" +
                                "To:   " + upgradedDbDir.getAbsolutePath() );
            try ( Store upgradedStore = originalStore.makeCopyAt( upgradedDbDir.toPath() ) )
            {
                EmbeddedDatabase.recreateSchema( upgradedStore, edition, neo4jConfigPath, workload.expectedSchema() );
            }
            System.out.println( "Upgrade complete" );
        }
    }
}
