/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.startup;

import org.apache.commons.lang3.SystemUtils;
import picocli.CommandLine;

import java.io.PrintStream;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.util.VisibleForTesting;

@CommandLine.Command(
        name = "Neo4j",
        description = "Neo4j database server CLI.",
        synopsisSubcommandLabel = "<COMMAND>"
)
class Neo4jCommand extends BootloaderCommand
{
    Neo4jCommand( Neo4jBootloaderContext ctx )
    {
        super( ctx );
    }

    @CommandLine.Command( name = "console", description = "Start server in console." )
    private static class Console extends BootCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( startOptions.expandCommands ).console();
        }
    }

    @CommandLine.Command( name = "start", description = "Start server as a daemon." )
    private static class Start extends BootCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( startOptions.expandCommands ).start();
        }
    }

    @CommandLine.Command( name = "stop", description = "Stop the server daemon." )
    private static class Stop extends BaseCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( false ).stop();
        }
    }

    @CommandLine.Command( name = "restart", description = "Restart the server daemon." )
    private static class Restart extends BootCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( startOptions.expandCommands ).restart();
        }
    }

    @CommandLine.Command( name = "status", description = "Get the status of the server." )
    private static class Status extends BaseCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( false ).status();
        }
    }

    @CommandLine.Command( name = "version", description = "Print version information and exit." )
    static class Version extends BaseCommand
    {
        @Override
        public Integer call()
        {
            bootloader.ctx.out.println( "neo4j " + org.neo4j.kernel.internal.Version.getNeo4jVersion() );
            return 0;
        }
    }

    //Windows service commands
    @CommandLine.Command( name = "install-service", description = "Install the Windows service." )
    private static class InstallService extends BootCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( startOptions.expandCommands ).installService();
        }
    }

    @CommandLine.Command( name = "uninstall-service", description = "Uninstall the Windows service." )
    private static class UninstallService extends BaseCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( false ).uninstallService();
        }
    }

    @CommandLine.Command( name = "update-service", description = "Update the Windows service." )
    private static class UpdateService extends BootCommand
    {
        @Override
        public Integer call()
        {
            return getBootloader( startOptions.expandCommands ).updateService();
        }
    }

    static CommandLine asCommandLine( Neo4jBootloaderContext ctx )
    {
        CommandLine cmd = new CommandLine( new Neo4jCommand( ctx ) )
                .addSubcommand( new Console() )
                .addSubcommand( new Start() )
                .addSubcommand( new Stop() )
                .addSubcommand( new Restart() )
                .addSubcommand( new Status( ) );

        if ( SystemUtils.IS_OS_WINDOWS )
        {
            cmd.addSubcommand( new InstallService() )
               .addSubcommand( new UninstallService() )
               .addSubcommand( new UpdateService() );
        }

        cmd.addSubcommand( "version", new Version(), "--version" )
           .addSubcommand( "help", new CommandLine.HelpCommand(), "--help" );

        return addDefaultOptions( cmd, ctx );
    }

    public static void main( String[] args )
    {
        int exitCode = Neo4jCommand.asCommandLine( new Neo4jBootloaderContext() ).execute( args );
        System.exit( exitCode );
    }

    static class Neo4jBootloaderContext extends BootloaderContext
    {
        Neo4jBootloaderContext()
        {
            super( EntryPoint.serviceloadEntryPoint() );
        }

        @VisibleForTesting
        Neo4jBootloaderContext( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup,
                Class<? extends EntryPoint> entrypoint )
        {
            super( out, err, envLookup, propLookup, entrypoint );
        }

        @Override
        protected Map<Setting<?>,Object> overriddenDefaultsValues()
        {
            return GraphDatabaseSettings.SERVER_DEFAULTS;
        }
    }
}
