/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Path;
import java.util.concurrent.Future;

import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileRepositorySerializer;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.string.UTF8;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.concurrent.ThreadingExtension;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

@TestDirectoryExtension
@ExtendWith( ThreadingExtension.class )
class FileRoleRepositoryTest
{
    private Path roleFile;
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private RoleRepository roleRepository;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private ThreadingRule threading;
    @Inject
    private FileSystemAbstraction fileSystem;

    @BeforeEach
    void setup()
    {
        roleFile = testDirectory.directoryPath( "dbms" ).resolve( "roles" );
        roleRepository = new FileRoleRepository( fileSystem, roleFile, logProvider );
    }

    @Test
    void shouldStoreAndRetrieveRolesByName() throws Exception
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "petra", "olivia" );
        roleRepository.create( role );

        // When
        RoleRecord result = roleRepository.getRoleByName( role.name() );

        // Then
        assertThat( result ).isEqualTo( role );
    }

    @Test
    void shouldPersistRoles() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "craig", "karl" );
        roleRepository.create( role );

        roleRepository = new FileRoleRepository( fileSystem, roleFile, logProvider );
        roleRepository.start();

        // When
        RoleRecord resultByName = roleRepository.getRoleByName( role.name() );

        // Then
        assertThat( resultByName ).isEqualTo( role );
    }

    @Test
    void shouldNotFindRoleAfterDelete() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "jake", "admin" );
        roleRepository.create( role );

        // When
        roleRepository.delete( role );

        // Then
        assertThat( roleRepository.getRoleByName( role.name() ) ).isNull();
    }

    @Test
    void shouldNotAllowComplexNames() throws Exception
    {
        // Given

        // When
        roleRepository.assertValidRoleName( "neo4j" );
        roleRepository.assertValidRoleName( "johnosbourne" );
        roleRepository.assertValidRoleName( "john_osbourne" );

        assertThatThrownBy( () -> roleRepository.assertValidRoleName( null ) )
                .isInstanceOf( InvalidArgumentsException.class )
                .hasMessage( "The provided role name is empty." );
        assertThatThrownBy( () -> roleRepository.assertValidRoleName( "" ) )
                .isInstanceOf( InvalidArgumentsException.class )
                .hasMessage( "The provided role name is empty." );
        assertThatThrownBy( () -> roleRepository.assertValidRoleName( ":" ) )
                .isInstanceOf( InvalidArgumentsException.class )
                .hasMessage( "Role name ':' contains illegal characters. Use simple ascii characters and numbers." );
        assertThatThrownBy( () -> roleRepository.assertValidRoleName( "john osbourne" ) )
                .isInstanceOf( InvalidArgumentsException.class )
                .hasMessage( "Role name 'john osbourne' contains illegal characters. Use simple ascii characters and numbers." );
        assertThatThrownBy( () -> roleRepository.assertValidRoleName( "john:osbourne" ) )
                .isInstanceOf( InvalidArgumentsException.class )
                .hasMessage( "Role name 'john:osbourne' contains illegal characters. Use simple ascii characters and numbers." );
    }

    @Test
    void shouldRecoverIfCrashedDuringMove() throws Throwable
    {
        // Given
        final IOException exception = new IOException( "simulated IO Exception on create" );
        FileSystemAbstraction crashingFileSystem =
                new DelegatingFileSystemAbstraction( fileSystem )
                {
                    @Override
                    public void renameFile( Path oldLocation, Path newLocation, CopyOption... copyOptions ) throws
                            IOException
                    {
                        if ( roleFile.getFileName().equals( newLocation.getFileName() ) )
                        {
                            throw exception;
                        }
                        super.renameFile( oldLocation, newLocation, copyOptions );
                    }
                };

        roleRepository = new FileRoleRepository( crashingFileSystem, roleFile, logProvider );
        roleRepository.start();
        RoleRecord role = new RoleRecord( "admin", "jake" );

        // When
        var e = assertThrows( IOException.class, () -> roleRepository.create( role ) );
        assertSame( exception, e );

        // Then
        assertFalse( crashingFileSystem.fileExists( roleFile ) );
        assertThat( crashingFileSystem.listFiles( roleFile.getParent() ).length ).isEqualTo( 0 );
    }

    @Test
    void shouldThrowIfUpdateChangesName() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "steve", "bob" );
        roleRepository.create( role );

        // When
        RoleRecord updatedRole = new RoleRecord( "admins", "steve", "bob" );
        assertThrows( IllegalArgumentException.class, () -> roleRepository.update( role, updatedRole ) );

        assertThat( roleRepository.getRoleByName( role.name() ) ).isEqualTo( role );
    }

    @Test
    void shouldThrowIfExistingRoleDoesNotMatch() throws Throwable
    {
        // Given
        RoleRecord role = new RoleRecord( "admin", "jake" );
        roleRepository.create( role );
        RoleRecord modifiedRole = new RoleRecord( "admin", "jake", "john" );

        // When
        RoleRecord updatedRole = new RoleRecord( "admin", "john" );
        assertThrows( ConcurrentModificationException.class, () -> roleRepository.update( modifiedRole, updatedRole ) );
    }

    @Test
    void shouldFailOnReadingInvalidEntries() throws Throwable
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        fileSystem.mkdirs( roleFile.getParent() );
        // First line is correctly formatted, second line has an extra field
        FileRepositorySerializer.writeToFile( fileSystem, roleFile, UTF8.encode(
                "neo4j:admin\n" +
                "admin:admin:\n" ) );

        // When
        roleRepository = new FileRoleRepository( fileSystem, roleFile, logProvider );

        var e = assertThrows( IllegalStateException.class, () -> roleRepository.start() );
        assertThat( e ).hasMessageStartingWith( "Failed to read role file '" );
        assertThat( roleRepository.numberOfRoles() ).isEqualTo( 0 );
        assertThat( logProvider ).forClass( FileRoleRepository.class ).forLevel( ERROR )
                .containsMessageWithArguments(
                        "Failed to read role file \"%s\" (%s)", roleFile.toAbsolutePath(),
                        "wrong number of line fields [line 2]" );
    }

    @Test
    void shouldNotAddEmptyUserToRole() throws Throwable
    {
        // Given
        fileSystem.mkdirs( roleFile.getParent() );
        FileRepositorySerializer.writeToFile( fileSystem, roleFile, UTF8.encode( "admin:neo4j\nreader:\n" ) );

        // When
        roleRepository = new FileRoleRepository( fileSystem, roleFile, logProvider );
        roleRepository.start();

        RoleRecord role = roleRepository.getRoleByName( "admin" );
        assertTrue( role.users().contains( "neo4j" ), "neo4j should be assigned to 'admin'" );
        assertEquals( 1, role.users().size(), "only one admin should exist" );

        role = roleRepository.getRoleByName( "reader" );
        assertTrue( role.users().isEmpty(), "no users should be assigned to 'reader'" );
    }

    @Test
    void shouldProvideRolesByUsernameEvenIfMidSetRoles() throws Throwable
    {
        // Given
        roleRepository = new FileRoleRepository( fileSystem, roleFile, logProvider );
        roleRepository.create( new RoleRecord( "admin", "oskar" ) );
        DoubleLatch latch = new DoubleLatch( 2 );

        // When
        Future<Object> setUsers = threading.execute( o ->
        {
            roleRepository.setRoles( new HangingListSnapshot( latch, 10L ) );
            return null;
        }, null );

        latch.startAndWaitForAllToStart();

        // Then
        assertThat( roleRepository.getRoleNamesByUsername( "oskar" ) ).contains( "admin" );

        latch.finish();
        setUsers.get();
    }

    static class HangingListSnapshot extends ListSnapshot<RoleRecord>
    {
        private final DoubleLatch latch;

        HangingListSnapshot( DoubleLatch latch, long timestamp )
        {
            super( timestamp, emptyList() );
            this.latch = latch;
        }

        @Override
        public long timestamp()
        {
            latch.start();
            latch.finishAndWaitForAllToFinish();
            return super.timestamp();
        }
    }
}
