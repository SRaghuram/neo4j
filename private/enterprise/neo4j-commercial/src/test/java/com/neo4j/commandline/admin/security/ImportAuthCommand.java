/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import com.neo4j.server.security.enterprise.CommercialSecurityModule;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
import com.neo4j.server.security.enterprise.auth.RealmLifecycle;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.UserRepository;

import static com.neo4j.server.security.enterprise.CommercialSecurityModule.ROLE_IMPORT_FILENAME;
import static com.neo4j.server.security.enterprise.CommercialSecurityModule.USER_IMPORT_FILENAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ImportAuthCommand implements AdminCommand
{
    public static final String COMMAND_NAME = CommercialSecurityModule.IMPORT_AUTH_COMMAND_NAME;
    public static final String USER_ARG_NAME = "users-file";
    public static final String ROLE_ARG_NAME = "roles-file";
    public static final String OFFLINE_ARG_NAME = "offline";
    public static final String RESET_ARG_NAME = "reset";
    // TODO: Currently it is not allowed to set SYSTEM_DATABASE_NAME as `active_database`. We need to work around this for --offline mode.
    public static final String IMPORT_SYSTEM_DATABASE_NAME = SYSTEM_DATABASE_NAME + ".import";

    private static final Arguments arguments = new Arguments()
            .withArgument( new OptionalNamedArg( USER_ARG_NAME, CommunitySecurityModule.USER_STORE_FILENAME, CommunitySecurityModule.USER_STORE_FILENAME,
                    "File name of user repository file to import." ))
            .withArgument( new OptionalNamedArg( ROLE_ARG_NAME, CommercialSecurityModule.ROLE_STORE_FILENAME, CommercialSecurityModule.ROLE_STORE_FILENAME,
                    "File name of role repository file to import." ))
            .withArgument( new OptionalBooleanArg( OFFLINE_ARG_NAME, false,
                    "If set to true the actual import will happen immediately into an offline system graph. " +
                            "Otherwise the actual import will happen on the next startup of Neo4j." ))
            .withArgument( new OptionalBooleanArg( RESET_ARG_NAME, false,
                    "If set to true all existing auth data in the system graph will be deleted before importing the new data. " +
                    "This only works in combination with --" + OFFLINE_ARG_NAME ));

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
            boolean offline = arguments.getBoolean( OFFLINE_ARG_NAME );
            boolean reset = arguments.getBoolean( RESET_ARG_NAME );

            if ( reset && !offline )
            {
                throw new IncorrectUsage( String.format( "%s only works in combination with %s", RESET_ARG_NAME, OFFLINE_ARG_NAME ) );
            }

            importAuth( arguments.get( USER_ARG_NAME ), arguments.get( ROLE_ARG_NAME ), offline, reset );
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

    Config loadNeo4jConfig()
    {
        return Config.fromFile( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .withHome( homeDir )
                .withConnectorsDisabled().build();
    }

    private void importAuth( String userFilename, String roleFilename, boolean offline, boolean reset ) throws Throwable
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

        if ( offline )
        {
            importAuthOffline( config, fileSystem, sourceUserFile, sourceRoleFile, reset );
        }
        else
        {
            importAuthOnDatabaseStart( fileSystem, parentFile, sourceUserFile, sourceRoleFile );
        }
    }

    private void importAuthOnDatabaseStart( FileSystemAbstraction fileSystem, File parentFile, File sourceUserFile, File sourceRoleFile ) throws IOException
    {
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

    private void importAuthOffline( Config config, FileSystemAbstraction fileSystem,
            File sourceUserFile, File sourceRoleFile, boolean reset ) throws Throwable
    {

        FileUserRepository userRepository = new FileUserRepository( fileSystem, sourceUserFile, NullLogProvider.getInstance() );
        FileRoleRepository roleRepository = new FileRoleRepository( fileSystem, sourceRoleFile, NullLogProvider.getInstance() );

        try ( Lifespan lifespan = new Lifespan( userRepository, roleRepository ) )
        {
            GraphDatabaseService systemDb = createSystemGraphDatabaseFacade( config );
            try
            {
                RealmLifecycle systemGraphRealm = createSystemGraphRealmForOfflineImport( systemDb, config, userRepository, roleRepository, reset );
                systemGraphRealm.initialize();
                systemGraphRealm.start();
                systemGraphRealm.stop();
                systemGraphRealm.shutdown();

                outsideWorld.stdOutLine( "Users and roles files imported into system graph." );
            }
            finally
            {
                systemDb.shutdown();
            }
        }
    }

    private GraphDatabaseService createSystemGraphDatabaseFacade( Config config )
    {
        File databaseDir = config.get( GraphDatabaseSettings.databases_root_path ).getAbsoluteFile();
        File systemDbStoreDir = new File( databaseDir, IMPORT_SYSTEM_DATABASE_NAME );

        // NOTE: We do not have the dependency to use CommercialGraphDatabaseFactory here, but EnterpriseGraphDatabaseFactory should work fine for this purpose
        CommercialGraphDatabaseFactory factory = new CommercialGraphDatabaseFactory();
        return factory.newEmbeddedDatabaseBuilder( systemDbStoreDir ).newGraphDatabase();
    }

    private RealmLifecycle createSystemGraphRealmForOfflineImport( GraphDatabaseService db, Config config,
            UserRepository importUserRepository, RoleRepository importRoleRepository,
            boolean shouldResetSystemGraphAuthBeforeImport ) throws IOException
    {

        SecurityLog securityLog = new SecurityLog( config, outsideWorld.fileSystem(), Runnable::run );

        DatabaseManager databaseManager = getDatabaseManager( db );
        return CommercialSecurityModule.createSystemGraphRealmForOfflineImport(
                config,
                securityLog,
                databaseManager,
                importUserRepository, importRoleRepository,
                shouldResetSystemGraphAuthBeforeImport );
    }

    private DatabaseManager getDatabaseManager( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
