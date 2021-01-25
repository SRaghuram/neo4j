/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.AutoDetectStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.options.Edition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

@Command( name = "upgrade-store", description = "Upgrades a Neo4j store, including rebuilding of indexes." )
public class UpgradeStoreCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( UpgradeStoreCommand.class );

    private static final String CMD_ORIGINAL_DB = "--original-db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_ORIGINAL_DB},
            description = "Neo4j database that needs to be upgraded. E.g. 'accesscontrol/' not 'accesscontrol/graph.db/'",
            title = "Original Neo4j DB " )
    @Required
    private File originalDbDir;

    private static final String CMD_WORKLOAD = "--workload";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WORKLOAD},
            description = "Path to workload configuration file",
            title = "Workload configuration" )
    @Required
    private String workloadName;

    private static final String CMD_EDITION = "--db-edition";
    @Option( type = OptionType.COMMAND,
            name = {CMD_EDITION},
            description = "Neo4j edition: COMMUNITY or ENTERPRISE",
            title = "Neo4j edition" )
    @Required
    private Edition edition;

    private static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
             name = {CMD_NEO4J_CONFIG},
             title = "Neo4j configuration file" )
    private File neo4jConfigFile;

    private static final String CMD_RECORD_FORMAT = "--record-format";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RECORD_FORMAT},
            title = "Store record format" )
    @Required
    private String recordFormat;

    @Override
    public void run()
    {
        LOG.debug( format( "Upgrading store for workload `%s`\n" +
                                    "In: `%s`\n",
                                    workloadName,
                                    originalDbDir.getAbsolutePath() ) );

        Path workDir = Paths.get( System.getProperty( "user.dir" ) );
        try ( Store originalStore = AutoDetectStore.createFrom( originalDbDir.toPath() );
              Resources resources = new Resources( workDir ) )
        {
            originalStore.assertDirectoryIsNeoStore();
            Workload workload = Workload.fromName( workloadName, resources, Deployment.embedded() );

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
                                  .withSetting( record_format, recordFormat )
                                  .writeToFile( neo4jConfigPath );
            }

            LOG.debug( "Checking schema..." );
            EmbeddedDatabase.verifySchema( originalStore, edition, neo4jConfigPath, workload.expectedSchema() );
            LOG.debug( "Recreating schema..." );
            EmbeddedDatabase.recreateSchema( originalStore, edition, neo4jConfigPath, workload.expectedSchema() );
            LOG.debug( "Upgrade complete" );
        }
    }

    public static List<String> argsFor( Path originalDbDir,
                                        String workloadName,
                                        Edition edition,
                                        Path neo4jConfigFile,
                                        String recordFormat )
    {
        List<String> args = Lists.newArrayList(
                "upgrade-store",
                CMD_ORIGINAL_DB,
                originalDbDir.toAbsolutePath().toString(),
                CMD_WORKLOAD,
                workloadName,
                CMD_EDITION,
                edition.name(),
                CMD_RECORD_FORMAT,
                recordFormat );
        if ( null != neo4jConfigFile )
        {
            args.add( CMD_NEO4J_CONFIG );
            args.add( neo4jConfigFile.toAbsolutePath().toString() );
        }
        return args;
    }
}
