/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package com.neo4j.commandline.admin.security;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.enterprise.auth.EnterpriseSecurityModule;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

public class ImportAuthCommand implements AdminCommand
{
    public static final String COMMAND_NAME = "import-auth";
    public static final String USER_IMPORT_FILENAME = ".users.import"; // TODO: Move to CommercialSecurityModule?
    public static final String ROLE_IMPORT_FILENAME = ".roles.import";
    public static final String USER_ARG_NAME = "users";
    public static final String ROLE_ARG_NAME = "roles";

    private static final Arguments arguments = new Arguments()
            .withArgument( new OptionalNamedArg( USER_ARG_NAME, CommunitySecurityModule.USER_STORE_FILENAME, CommunitySecurityModule.USER_STORE_FILENAME,
                    "File name of user repository file to import." ))
            .withArgument( new OptionalNamedArg( ROLE_ARG_NAME, EnterpriseSecurityModule.ROLE_STORE_FILENAME, EnterpriseSecurityModule.ROLE_STORE_FILENAME,
                    "File name of role repository file to import." ));

    private final Path homeDir;
    private final Path configDir;
    private OutsideWorld outsideWorld;

    ImportAuthCommand( Path homeDir, Path configDir, OutsideWorld outsideWorld )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.outsideWorld = outsideWorld;
    }

    public static Arguments arguments()
    {
        return arguments;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        try
        {
            arguments.parse( args );
            importAuth( arguments.get( USER_ARG_NAME ), arguments.get( ROLE_ARG_NAME ) );
        }
        catch ( IncorrectUsage | CommandFailed e )
        {
            throw e;
        }
        catch ( Throwable throwable )
        {
            throw new CommandFailed( throwable.getMessage(), new RuntimeException( throwable ) );
        }
    }

    private void importAuth( String userFilename, String roleFilename ) throws Throwable
    {
        FileSystemAbstraction fileSystem = outsideWorld.fileSystem();
        Config config = loadNeo4jConfig();

        Path sourceUserPath = Paths.get( userFilename );
        Path sourceRolePath = Paths.get( roleFilename );
        File parentFile = CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile();

        File sourceUserFile;
        File sourceRoleFile;

        sourceUserFile = sourceUserPath.getParent() == null ?
                         new File( parentFile, userFilename ) :
                         sourceUserPath.toFile();
        sourceRoleFile = sourceRolePath.getParent() == null ?
                         new File( parentFile, roleFilename ) :
                         sourceRolePath.toFile();

        File targetUserFile = new File( parentFile, USER_IMPORT_FILENAME );
        File targetRoleFile = new File( parentFile, ROLE_IMPORT_FILENAME );

        if ( fileSystem.fileExists( targetUserFile ) )
        {
            fileSystem.deleteFile( targetUserFile );
        }
        if ( fileSystem.fileExists( targetRoleFile ) )
        {
            fileSystem.deleteFile( targetRoleFile );
        }

        fileSystem.copyFile( sourceUserFile, targetUserFile );
        fileSystem.copyFile( sourceRoleFile, targetRoleFile );

        outsideWorld.stdOutLine( "Users and roles files copied to import files. " +
                "Please restart the database with configuration setting " + SecuritySettings.auth_provider.name() + "=" +
                SecuritySettings.SYSTEM_GRAPH_REALM_NAME + " to complete the import." );
    }

    Config loadNeo4jConfig()
    {
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withConnectorsDisabled().build();
    }
}
