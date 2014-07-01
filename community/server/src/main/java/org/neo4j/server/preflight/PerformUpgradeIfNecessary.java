/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.preflight;

import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.configForStoreDir;

import java.io.File;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storemigration.CurrentDatabase;
import org.neo4j.kernel.impl.storemigration.StoreMigrationTool;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.Monitor;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;

public class PerformUpgradeIfNecessary implements PreflightTask
{
    private String failureMessage = "Unable to upgrade database";
    private final Configuration config;
    private final Map<String, String> dbConfig;
    private final ConsoleLogger log;
    private final Monitor monitor;

    public PerformUpgradeIfNecessary( Configuration serverConfig, Map<String, String> dbConfig,
            Logging logging, StoreUpgrader.Monitor monitor )
    {
        this.config = serverConfig;
        this.dbConfig = dbConfig;
        this.monitor = monitor;
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public boolean run()
    {
        try
        {
            String dbLocation = new File( config.getString( Configurator.DATABASE_LOCATION_PROPERTY_KEY ) )
                    .getAbsolutePath();

            if ( new CurrentDatabase(new StoreVersionCheck( new DefaultFileSystemAbstraction() ) ).storeFilesAtCurrentVersion( new File( dbLocation ) ) )
            {
                return true;
            }

            File storeDir = new File( dbLocation );
            FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );
            if ( !upgradableDatabase.storeFilesUpgradeable( storeDir ) )
            {
                return true;
            }

            try
            {
                new StoreMigrationTool().run( dbLocation,
                        configForStoreDir( new Config( dbConfig ), storeDir ), StringLogger.SYSTEM, monitor );
            }
            catch ( UpgradeNotAllowedByConfigurationException e )
            {
                log.log( e.getMessage() );
                // TODO Do my eyes deceive me? We have a logger AND we print to stdout explicitly? That can't be right...
                System.out.println( e.getMessage() );
                failureMessage = e.getMessage();
                return false;
            }
            catch ( StoreUpgrader.UnableToUpgradeException e )
            {
                log.error( "Unable to upgrade store", e );
                return false;
            }
            return true;
        }
        catch ( Exception e )
        {
            log.error( "Unknown error", e );
            return false;
        }
    }

    @Override
    public String getFailureMessage()
    {
        return failureMessage;
    }
}
