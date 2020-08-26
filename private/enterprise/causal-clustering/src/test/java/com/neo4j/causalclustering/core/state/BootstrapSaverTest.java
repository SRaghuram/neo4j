/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@TestDirectoryExtension
@ExtendWith( DefaultFileSystemExtension.class )
class BootstrapSaverTest
{
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory dir;

    enum Layout
    {
        FLAT
                {
                    @Override
                    DatabaseLayout get( Path home )
                    {
                        return DatabaseLayout.ofFlat( home.resolve( "neo4j" ) );
                    }
                },
        STANDARD
                {
                    @Override
                    DatabaseLayout get( Path home )
                    {
                        return Neo4jLayout.of( home ).databaseLayout( "neo4j" );
                    }
                };

        abstract DatabaseLayout get( Path home );
    }

    @ParameterizedTest
    @EnumSource( Layout.class )
    void testSaveRestoreClean( Layout layout ) throws IOException
    {
        // given
        DatabaseLayout databaseLayout = layout.get( dir.homePath() );
        BootstrapSaver saver = new BootstrapSaver( fs, nullLogProvider() );

        fs.mkdirs( dir.homePath() );

        FakeStore store = new FakeStore( fs, databaseLayout );
        store.create();

        // when: saving the database
        saver.save( databaseLayout );

        // then: should have been moved to temp-save
        store.assertMissing();
        assertTrue( fs.fileExists( databaseLayout.databaseDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertTrue( fs.fileExists( databaseLayout.getTransactionLogsDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: restoring
        saver.restore( databaseLayout );

        // then: should be back in standard location
        store.assertExists();
        assertFalse( fs.fileExists( databaseLayout.databaseDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( databaseLayout.getTransactionLogsDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: cleaning without anything to clean
        saver.clean( databaseLayout );

        // then: should have no effect
        store.assertExists();
        assertFalse( fs.fileExists( databaseLayout.databaseDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( databaseLayout.getTransactionLogsDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
    }

    @ParameterizedTest
    @EnumSource( Layout.class )
    void testSaveCleanRestore( Layout layout ) throws IOException
    {
        // given
        DatabaseLayout databaseLayout = layout.get( dir.homePath() );
        BootstrapSaver saver = new BootstrapSaver( fs, nullLogProvider() );

        fs.mkdirs( dir.homePath() );

        FakeStore store = new FakeStore( fs, databaseLayout );
        store.create();

        // when: saving the database
        saver.save( databaseLayout );

        // then: should have been moved
        store.assertMissing();
        assertTrue( fs.fileExists( databaseLayout.databaseDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertTrue( fs.fileExists( databaseLayout.getTransactionLogsDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: simulate a store copy and clean of temp-save
        store.create();
        saver.clean( databaseLayout );

        // then: store should exist but not temp-save
        store.assertExists();
        assertFalse( fs.fileExists( databaseLayout.databaseDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( databaseLayout.getTransactionLogsDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: restoring - typically after a restart
        saver.restore( databaseLayout );

        // then: this should not change anything
        store.assertExists();
        assertFalse( fs.fileExists( databaseLayout.databaseDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( databaseLayout.getTransactionLogsDirectory().resolve( TEMP_SAVE_DIRECTORY_NAME ) ) );
    }

    private static class FakeStore
    {
        private final FileSystemAbstraction fs;
        private final DatabaseLayout layout;
        private final List<FsNode> fsNodes;

        FakeStore( FileSystemAbstraction fs, DatabaseLayout layout )
        {
            this.fs = fs;
            this.layout = layout;

            TransactionLogFilesHelper txHelper = new TransactionLogFilesHelper( null, layout.getTransactionLogsDirectory() );

            Path schema = layout.databaseDirectory().resolve( "schema" );
            Path index = layout.databaseDirectory().resolve( "index" );

            this.fsNodes = List.of(
                    new FsNode( layout.metadataStore(), false ),
                    new FsNode( layout.nodeStore(), false ),

                    new FsNode( schema, true ),
                    new FsNode( schema.resolve( "schema-0" ), false ),
                    new FsNode( schema.resolve(  "schema-1" ), false ),

                    new FsNode( index, true ),
                    new FsNode( index.resolve( "index-0" ), false ),
                    new FsNode( index.resolve( "index-1" ), false ),

                    new FsNode( txHelper.getLogFileForVersion( 0 ), false ),
                    new FsNode( txHelper.getLogFileForVersion( 1 ), false )
            );
        }

        void create() throws IOException
        {
            fs.mkdirs( layout.databaseDirectory() );
            fs.mkdirs( layout.getTransactionLogsDirectory() );

            for ( FsNode fsNode : fsNodes )
            {
                fsNode.create( fs );
            }
        }

        void assertExists()
        {
            for ( FsNode fsNode : fsNodes )
            {
                fsNode.assertExistence( fs, true );
            }
        }

        void assertMissing()
        {
            for ( FsNode fsNode : fsNodes )
            {
                fsNode.assertExistence( fs, false );
            }
        }
    }

    private static class FsNode
    {
        private final Path path;
        private final boolean isDirectory;

        FsNode( Path path, boolean isDirectory )
        {
            this.path = path;
            this.isDirectory = isDirectory;
        }

        void create( FileSystemAbstraction fs ) throws IOException
        {
            if ( isDirectory )
            {
                fs.mkdirs( path );
            }
            else
            {
                fs.openAsOutputStream( path, false ).close();
            }
        }

        void assertExistence( FileSystemAbstraction fs, boolean shouldExist )
        {
            if ( shouldExist )
            {
                assertTrue( fs.fileExists( path ), "Should exist: " + path );
                assertEquals( isDirectory,
                        fs.isDirectory( path ), (isDirectory ? "Should be directory: " : "Should not be directory: ") + path );
            }
            else
            {
                assertFalse( fs.fileExists( path ), "Should not exist: " + path );
            }
        }
    }
}
