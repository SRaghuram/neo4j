/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import com.neo4j.server.security.enterprise.CommercialSecurityModule;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith( TestDirectoryExtension.class )
class ImportAuthCommandTest
{
    private static final String ALTERNATIVE_USER_STORE_FILENAME = "users_to_import";
    private static final String ALTERNATIVE_ROLE_STORE_FILENAME = "roles_to_import";

    private ImportAuthCommand importAuth;
    private File userImportFile;
    private File roleImportFile;
    private File altUserStoreFile;
    private File altRoleStoreFile;
    private Config config;
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDir;

    @BeforeEach
    void setup() throws IOException, InvalidArgumentsException
    {
        fileSystem = testDir.getFileSystem();
        OutsideWorld mock = mock( OutsideWorld.class );
        when( mock.fileSystem() ).thenReturn( fileSystem );
        File configDirectory = testDir.directory( "conf" );
        fileSystem.mkdirs( configDirectory );
        fileSystem.create( new File( configDirectory, Config.DEFAULT_CONFIG_FILE_NAME ) ).close();
        importAuth = new ImportAuthCommand( testDir.directory( "home" ).toPath(), configDirectory.toPath(), mock );
        config = importAuth.loadNeo4jConfig();
        UserRepository users = CommunitySecurityModule.getUserRepository( config, NullLogProvider.getInstance(), fileSystem );
        users.create(
                new User.Builder( "jake", LegacyCredential.forPassword( "123" ) )
                        .withRequiredPasswordChange( false )
                        .build()
        );
        RoleRepository roles = CommercialSecurityModule.getRoleRepository( config, NullLogProvider.getInstance(), fileSystem );
        roles.create(
                new RoleRecord.Builder().withName( "sorcerer" ).withUser( "jake" ).build()
        );
        File userStoreFile = CommunitySecurityModule.getUserRepositoryFile( config );
        File roleStoreFile = CommercialSecurityModule.getRoleRepositoryFile( config );
        File parentFolder = userStoreFile.getParentFile();
        altUserStoreFile = new File( parentFolder, ALTERNATIVE_USER_STORE_FILENAME );
        altRoleStoreFile = new File( parentFolder, ALTERNATIVE_ROLE_STORE_FILENAME );
        fileSystem.copyFile( userStoreFile, altUserStoreFile );
        fileSystem.copyFile( roleStoreFile, altRoleStoreFile );
        userImportFile = new File( parentFolder, CommercialSecurityModule.USER_IMPORT_FILENAME );
        roleImportFile = new File( parentFolder, CommercialSecurityModule.ROLE_IMPORT_FILENAME );
    }

    @Test
    void shouldCreateImportFilesWithoutArguments() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( userImportFile ) );
        assertFalse( fileSystem.fileExists( roleImportFile ) );

        // When
        String[] arguments = {};
        importAuth.execute( arguments );

        // Then
        assertUserImportFile( "jake" );
        assertRoleImportFile( "sorcerer", "jake" );
    }

    @Test
    void shouldCreateImportFilesWithGivenArguments() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( userImportFile ) );
        assertFalse( fileSystem.fileExists( roleImportFile ) );

        // When
        String[] arguments = {
                "--" + ImportAuthCommand.USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ImportAuthCommand.ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME
        };
        importAuth.execute( arguments );

        // Then
        assertUserImportFile( "jake" );
        assertRoleImportFile( "sorcerer", "jake" );
    }

    @Test
    void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new ImportAuthCommandProvider(), ps::println );

            assertEquals( String.format(
                    "usage: neo4j-admin import-auth [--users-file=<auth>] [--roles-file=<roles>]%n" +
                            "                               [--offline[=<true|false>]]%n" +
                            "                               [--reset[=<true|false>]]%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Import users and roles from files into the system graph, for example when%n" +
                            "upgrading to Neo4j 3.5 Commercial Edition. This can be used to migrate auth data%n" +
                            "from the flat files used as storage by the old native auth provider into the%n" +
                            "'system-graph' auth provider.%n" +
                            "%n" +
                            "options:%n" +
                            "  --users-file=<auth>      File name of user repository file to import.%n" +
                            "                           [default:auth]%n" +
                            "  --roles-file=<roles>     File name of role repository file to import.%n" +
                            "                           [default:roles]%n" +
                            "  --offline=<true|false>   If set to true the actual import will happen%n" +
                            "                           immediately into an offline system graph. Otherwise%n" +
                            "                           the actual import will happen on the next startup of%n" +
                            "                           Neo4j. [default:false]%n" +
                            "  --reset=<true|false>     If set to true all existing auth data in the system%n" +
                            "                           graph will be deleted before importing the new data.%n" +
                            "                           This only works in combination with --offline%n" +
                            "                           [default:false]%n" ),
                    baos.toString() );
        }
    }

    private void assertUserImportFile( String username ) throws Throwable
    {
        assertTrue( fileSystem.fileExists( userImportFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, userImportFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        assertThat( userRepository.getAllUsernames(), containsInAnyOrder( username ) );
    }

    private void assertRoleImportFile( String roleName, String username ) throws Throwable
    {
        assertTrue( fileSystem.fileExists( roleImportFile ) );
        FileRoleRepository roleRepository = new FileRoleRepository( fileSystem, roleImportFile,
                NullLogProvider.getInstance() );
        roleRepository.start();
        assertThat( roleRepository.getRoleNamesByUsername( username ), containsInAnyOrder( roleName ) );
    }
}
