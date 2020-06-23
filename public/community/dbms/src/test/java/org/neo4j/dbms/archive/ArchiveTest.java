/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.dbms.archive;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.Files.isDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.function.Predicates.alwaysFalse;
import static org.neo4j.internal.helpers.collection.Pair.pair;

@Neo4jLayoutExtension
class ArchiveTest
{
    @Inject
    private TestDirectory testDirectory;

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripAnEmptyDirectory( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripASingleFile( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), "text".getBytes() );

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripAnEmptyFile( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), new byte[0] );

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripFilesWithDifferentContent( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), "text".getBytes() );
        Files.write( directory.resolve( "another-file" ), "some-different-text".getBytes() );

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripEmptyDirectories( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "a-subdirectory" );
        Files.createDirectories( subdir );
        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripFilesInDirectories( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "a-subdirectory" );
        Files.createDirectories( subdir );
        Files.write( subdir.resolve( "a-file" ), "text".getBytes() );
        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldCopeWithLongPaths( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "a/very/long/path/which/is/not/realistic/for/a/database/today/but/which" +
                "/ensures/that/we/dont/get/caught/out/at/in/the/future/the/point/being/that/there/are/multiple/tar" +
                "/formats/some/of/which/do/not/cope/with/long/paths" );
        Files.createDirectories( subdir );
        Files.write( subdir.resolve( "a-file" ), "text".getBytes() );
        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldExcludeFilesMatchedByTheExclusionPredicate( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), new byte[0] );
        Files.write( directory.resolve( "another-file" ), new byte[0] );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( directory, directory, archive, compressionFormat, path -> path.getFileName().toString().equals( "another-file" ) );
        File txRootDirectory = testDirectory.directory( "tx-root_directory" );
        DatabaseLayout databaseLayout = layoutWithCustomTxRoot( txRootDirectory,"the-new-directory" );
        new Loader().load( archive, databaseLayout );

        Path expectedOutput = testDirectory.directory( "expected-output" ).toPath();
        Files.createDirectories( expectedOutput );
        Files.write( expectedOutput.resolve( "a-file" ), new byte[0] );

        assertEquals( describeRecursively( expectedOutput ), describeRecursively( databaseLayout.databaseDirectory() ) );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldExcludeWholeDirectoriesMatchedByTheExclusionPredicate( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "subdir" );
        Files.createDirectories( subdir );
        Files.write( subdir.resolve( "a-file" ), new byte[0] );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( directory, directory, archive, compressionFormat, path -> path.getFileName().toString().equals( "subdir" ) );
        File txLogsRoot = testDirectory.directory( "txLogsRoot" );
        DatabaseLayout databaseLayout = layoutWithCustomTxRoot( txLogsRoot,"the-new-directory" );

        new Loader().load( archive, databaseLayout );

        Path expectedOutput = testDirectory.directory( "expected-output" ).toPath();
        Files.createDirectories( expectedOutput );

        assertEquals( describeRecursively( expectedOutput ), describeRecursively( databaseLayout.databaseDirectory() ) );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void dumpAndLoadTransactionLogsFromCustomLocations( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        File txLogsRoot = testDirectory.directory( "txLogsRoot" );
        DatabaseLayout testDatabaseLayout = layoutWithCustomTxRoot( txLogsRoot,"testDatabase" );
        Files.createDirectories( testDatabaseLayout.databaseDirectory() );
        Path txLogsDirectory = testDatabaseLayout.getTransactionLogsDirectory();
        Files.createDirectories( txLogsDirectory );
        Files.write( testDatabaseLayout.databaseDirectory().resolve( "dbfile" ), new byte[0] );
        Files.write( txLogsDirectory.resolve( TransactionLogFilesHelper.DEFAULT_NAME + ".0" ), new byte[0] );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( testDatabaseLayout.databaseDirectory(), txLogsDirectory, archive, compressionFormat, alwaysFalse() );

        File newTxLogsRoot = testDirectory.directory( "newTxLogsRoot" );
        DatabaseLayout newDatabaseLayout = layoutWithCustomTxRoot( newTxLogsRoot,"the-new-database" );

        new Loader().load( archive, newDatabaseLayout );

        Path expectedOutput = testDirectory.directory( "expected-output" ).toPath();
        Files.write( expectedOutput.resolve( "dbfile" ), new byte[0] );

        Path expectedTxLogs = testDirectory.directory( "expectedTxLogs" ).toPath();
        Files.write( expectedTxLogs.resolve( TransactionLogFilesHelper.DEFAULT_NAME + ".0" ), new byte[0] );

        assertEquals( describeRecursively( expectedOutput ), describeRecursively( newDatabaseLayout.databaseDirectory() ) );
        assertEquals( describeRecursively( expectedTxLogs ), describeRecursively( newDatabaseLayout.getTransactionLogsDirectory() ) );
    }

    private DatabaseLayout layoutWithCustomTxRoot( File txLogsRoot, String databaseName )
    {
        Config config = Config.newBuilder()
                .set( neo4j_home, testDirectory.homeDir().toPath() )
                .set( transaction_logs_root_path, txLogsRoot.toPath().toAbsolutePath() )
                .set( default_database, databaseName )
                .build();
        return DatabaseLayout.of( config );
    }

    private void assertRoundTrips( Path oldDirectory, CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( oldDirectory, oldDirectory, archive, compressionFormat, alwaysFalse() );
        File newDirectory = testDirectory.file( "the-new-directory" );
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( newDirectory.toPath() );
        new Loader().load( archive, databaseLayout );

        assertEquals( describeRecursively( oldDirectory ), describeRecursively( newDirectory.toPath() ) );
    }

    private Map<Path,Description> describeRecursively( Path directory ) throws IOException
    {
        try ( Stream<Path> walk = Files.walk( directory ) )
        {
            return walk.map( path -> pair( directory.relativize( path ), describe( path ) ) )
                    .collect( HashMap::new, ( pathDescriptionHashMap, pathDescriptionPair ) ->
                                    pathDescriptionHashMap.put( pathDescriptionPair.first(), pathDescriptionPair.other() ),
                    HashMap::putAll );
        }
    }

    private Description describe( Path file )
    {
        try
        {
            return isDirectory( file ) ? new DirectoryDescription() : new FileDescription( Files.readAllBytes( file ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private interface Description
    {
    }

    private static class DirectoryDescription implements Description
    {
        @Override
        public boolean equals( Object o )
        {
            return this == o || !(o == null || getClass() != o.getClass());
        }

        @Override
        public int hashCode()
        {
            return 1;
        }
    }

    private static class FileDescription implements Description
    {
        private final byte[] bytes;

        FileDescription( byte[] bytes )
        {
            this.bytes = bytes;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            FileDescription that = (FileDescription) o;
            return Arrays.equals( bytes, that.bytes );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( bytes );
        }
    }
}
