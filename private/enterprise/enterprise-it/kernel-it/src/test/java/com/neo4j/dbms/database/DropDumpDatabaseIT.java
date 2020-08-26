/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.database_dumps_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

@EnterpriseDbmsExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
public class DropDumpDatabaseIT
{
    @Inject
    DatabaseManagementService managementService;
    @Inject
    Neo4jLayout neo4jLayout;
    @Inject
    TestDirectory testDirectory;
    @Inject
    FileSystemAbstraction fsa;
    @Inject
    Config config;

    @Test
    void dropDumpLoadDatabase() throws IOException
    {
        // given
        var dbName = "foo";
        managementService.createDatabase( dbName );
        var fooDb = managementService.database( dbName );
        var testLabel = Label.label( "test" );
        writeNode( fooDb, testLabel );
        var systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        var dumpRoots = config.get( database_dumps_root_path );

        // when
        dropDump( systemDb, fooDb.databaseName() );

        // then
        var dumpsArr = fsa.listFiles( dumpRoots, ( dir, name ) -> name.startsWith( dbName ) );
        var fooDump = Arrays.stream( dumpsArr ).findFirst().orElseThrow( AssertionError::new );

        // given
        var configDir = testDirectory.directory( "configDir" ).toPath();
        var homeDir = config.get( neo4j_home );
        setupConfFile( configDir );

        // when
        load( dbName, fooDump, homeDir, configDir );
        managementService.createDatabase( dbName );
        fooDb = managementService.database( dbName );

        // then
        assertThat( nodeExistsWithLabel( fooDb, testLabel ) ).isTrue();
    }

    void dropDump( GraphDatabaseService systemDb, String dbName )
    {
        try ( var tx = systemDb.beginTx() )
        {
            tx.execute( "DROP DATABASE `" + dbName + "` DUMP DATA" );
            tx.commit();
        }
    }

    void writeNode( GraphDatabaseService db, Label label )
    {
        try ( var tx = db.beginTx() )
        {
            tx.createNode( label );
            tx.commit();
        }
    }

    boolean nodeExistsWithLabel( GraphDatabaseService db, Label label )
    {
        var result = false;
        try ( var tx = db.beginTx() )
        {
            result = tx.findNodes( label ).stream().findFirst().isPresent();
        }
        return result;
    }

    void setupConfFile( Path configDir ) throws IOException
    {
        var configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        var databasesRootValue = neo4jLayout.databasesDirectory().toString()
                                            .replace( '\\', '/' );
        var databasesRootKey = databases_root_path.name();
        var databasesRootSetting = String.format( "%s=%s", databasesRootKey, databasesRootValue );
        Files.write( configFile, List.of( databasesRootSetting ) );
    }

    private void load( String database, Path dump, Path neo4jHome, Path configDir )
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new LoadCommand( context, new Loader( System.out ) );

        String[] args = {"--database=" + database, "--from=" + dump.toAbsolutePath()};
        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
