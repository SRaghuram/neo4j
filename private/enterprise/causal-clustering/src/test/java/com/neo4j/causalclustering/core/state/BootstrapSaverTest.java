/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

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
                    DatabaseLayout get( File home )
                    {
                        return DatabaseLayout.ofFlat( new File( home, "neo4j" ) );
                    }
                },
        STANDARD
                {
                    @Override
                    DatabaseLayout get( File home )
                    {
                        return Neo4jLayout.of( home ).databaseLayout( "neo4j" );
                    }
                };

        abstract DatabaseLayout get( File home );
    }

    @ParameterizedTest
    @EnumSource( Layout.class )
    void testSaveRestoreClean( Layout layout ) throws IOException
    {
        // given
        DatabaseLayout databaseLayout = layout.get( dir.homeDir() );
        BootstrapSaver saver = new BootstrapSaver( fs, nullLogProvider() );

        fs.mkdirs( dir.homeDir() );

        FakeStore store = new FakeStore( fs, databaseLayout );
        store.create();

        // when: saving the database
        saver.save( databaseLayout );

        // then: should have been moved to temp-save
        store.assertMissing();
        assertTrue( fs.fileExists( new File( databaseLayout.databaseDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertTrue( fs.fileExists( new File( databaseLayout.getTransactionLogsDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: restoring
        saver.restore( databaseLayout );

        // then: should be back in standard location
        store.assertExists();
        assertFalse( fs.fileExists( new File( databaseLayout.databaseDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( new File( databaseLayout.getTransactionLogsDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: cleaning without anything to clean
        saver.clean( databaseLayout );

        // then: should have no effect
        store.assertExists();
        assertFalse( fs.fileExists( new File( databaseLayout.databaseDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( new File( databaseLayout.getTransactionLogsDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
    }

    @ParameterizedTest
    @EnumSource( Layout.class )
    void testSaveCleanRestore( Layout layout ) throws IOException
    {
        // given
        DatabaseLayout databaseLayout = layout.get( dir.homeDir() );
        BootstrapSaver saver = new BootstrapSaver( fs, nullLogProvider() );

        fs.mkdirs( dir.homeDir() );

        FakeStore store = new FakeStore( fs, databaseLayout );
        store.create();

        // when: saving the database
        saver.save( databaseLayout );

        // then: should have been moved
        store.assertMissing();
        assertTrue( fs.fileExists( new File( databaseLayout.databaseDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertTrue( fs.fileExists( new File( databaseLayout.getTransactionLogsDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: simulate a store copy and clean of temp-save
        store.create();
        saver.clean( databaseLayout );

        // then: store should exist but not temp-save
        store.assertExists();
        assertFalse( fs.fileExists( new File( databaseLayout.databaseDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( new File( databaseLayout.getTransactionLogsDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );

        // when: restoring - typically after a restart
        saver.restore( databaseLayout );

        // then: this should not change anything
        store.assertExists();
        assertFalse( fs.fileExists( new File( databaseLayout.databaseDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
        assertFalse( fs.fileExists( new File( databaseLayout.getTransactionLogsDirectory(), CausalClusteringInternalSettings.TEMP_SAVE_DIRECTORY_NAME ) ) );
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

            File schema = new File( layout.databaseDirectory(), "schema" );
            File index = new File( layout.databaseDirectory(), "index" );

            this.fsNodes = List.of(
                    new FsNode( layout.metadataStore(), false ),
                    new FsNode( layout.nodeStore(), false ),

                    new FsNode( schema, true ),
                    new FsNode( new File( schema,"schema-0" ), false ),
                    new FsNode( new File( schema, "schema-1" ), false ),

                    new FsNode( index, true ),
                    new FsNode( new File( index, "index-0" ), false ),
                    new FsNode( new File( index, "index-1" ), false ),

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
        private final File file;
        private final boolean isDirectory;

        FsNode( File file, boolean isDirectory )
        {
            this.file = file;
            this.isDirectory = isDirectory;
        }

        void create( FileSystemAbstraction fs ) throws IOException
        {
            if ( isDirectory )
            {
                fs.mkdirs( file );
            }
            else
            {
                fs.openAsOutputStream( file, false ).close();
            }
        }

        void assertExistence( FileSystemAbstraction fs, boolean shouldExist )
        {
            if ( shouldExist )
            {
                assertTrue( fs.fileExists( file ), "Should exist: " + file );
                assertEquals( isDirectory, fs.isDirectory( file ), (isDirectory ? "Should be directory: " : "Should not be directory: ") + file );
            }
            else
            {
                assertFalse( fs.fileExists( file ), "Should not exist: " + file );
            }
        }
    }
}
