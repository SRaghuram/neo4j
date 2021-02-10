/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.local;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestDirectoryExtension
class FileManagerTest
{
    @Inject
    FileSystemAbstraction fs;
    @Inject
    TestDirectory testDirectory;

    private FileManager fileManager;

    @BeforeEach
    void setUp()
    {
        fileManager = new FileManager( fs );
    }

    private static Stream<Arguments> patterns()
    {
        return Stream.of(
                Arguments.of( (Function<FileManager,ThrowingFunction<Path,Path,IOException>>) f -> f::nextWorkingDir, FileManager.WORKING_DIR_PATTERN ),
                Arguments.of( (Function<FileManager,ThrowingFunction<Path,Path,IOException>>) f -> f::nextErrorDir, FileManager.ERROR_DIR_PATTERN )
        );
    }

    @ParameterizedTest
    @MethodSource( "patterns" )
    void shouldCreateNewWorkingDir( Function<FileManager,ThrowingFunction<Path,Path,IOException>> nextDir, String pattern ) throws IOException
    {
        ThrowingFunction<Path,Path,IOException> nextDirProvider = nextDir.apply( fileManager );

        var path = testDirectory.directory( "foo" );
        for ( int i = 0; i < 10; i++ )
        {
            var nexDir = nextDirProvider.apply( path );
            fs.mkdir( nexDir );
            fs.write( nexDir.resolve( "file" ) ).write( ByteBuffer.wrap( "write".getBytes() ) );
            var expectedName = format( pattern, "foo", i );
            assertThat( nexDir ).exists()
                    .matches( p -> p.getFileName().toString().equals( expectedName ), "Expected file name to match " + expectedName );
        }
    }

    @Test
    void shouldCorrectlyDetermineExistingPaths()
    {
        var exitingPath = testDirectory.directory( "foo" );
        var notExitingPath = Path.of( UUID.randomUUID().toString() );
        assertThat( fileManager.exists( exitingPath ) ).isTrue();
        assertThat( fileManager.exists( notExitingPath ) ).isFalse();
    }

    @Test
    void shouldDetermineIfDirectoryDoesNotExistOrIsEmpty() throws IOException
    {
        var emptyExistingDir = testDirectory.directory( "foo" );
        var nonEmptyDir = testDirectory.directory( "bar" );
        testDirectory.directory( "bar", "bar" );
        var exitingFile = testDirectory.createFile( "baz" );
        var notExitingPath = Path.of( UUID.randomUUID().toString() );

        assertThat( fileManager.directoryDoesNotExistOrIsEmpty( emptyExistingDir ) ).isTrue();
        assertThat( fileManager.directoryDoesNotExistOrIsEmpty( notExitingPath ) ).isTrue();
        assertThat( fileManager.directoryDoesNotExistOrIsEmpty( nonEmptyDir ) ).isFalse();
        assertThat( fileManager.directoryDoesNotExistOrIsEmpty( exitingFile ) ).isFalse();
    }

    @Test
    void shouldMoveDirAndContent() throws IOException
    {
        var originalPath = testDirectory.directory( "foo" );
        var subPath = testDirectory.directory( "foo", "foo" );
        fs.write( originalPath.resolve( "bar" ) ).write( ByteBuffer.wrap( "text".getBytes() ) );
        var destinationPath = testDirectory.homePath().resolve( "bar" );
        var destinationSubPath = destinationPath.resolve( "foo" );

        assertThat( fs.fileExists( originalPath ) ).isTrue();
        assertThat( fs.fileExists( subPath ) ).isTrue();
        assertThat( fs.fileExists( destinationPath ) ).isFalse();
        assertThat( fs.fileExists( destinationSubPath ) ).isFalse();

        Arrays.stream( fs.listFiles( originalPath ) ).forEach( System.out::println );
        Arrays.stream( fs.listFiles( testDirectory.homePath() ) ).forEach( System.out::println );
        fileManager.copyDelete( originalPath, destinationPath );

        Arrays.stream( fs.listFiles( testDirectory.homePath() ) ).forEach( System.out::println );

        assertThat( fs.fileExists( originalPath ) ).isFalse();
        assertThat( fs.fileExists( subPath ) ).isFalse();
        assertThat( fs.fileExists( destinationPath ) ).isTrue();
        assertThat( fs.fileExists( destinationSubPath ) ).isTrue();
    }
}
