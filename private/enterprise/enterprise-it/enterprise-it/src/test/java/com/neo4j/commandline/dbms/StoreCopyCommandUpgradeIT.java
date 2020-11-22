/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.dbms.commandline.StoreCopyCommand;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.storeupgrade.StoreUpgradeIT;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStore;
import static org.neo4j.logging.NullLogProvider.getInstance;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class StoreCopyCommandUpgradeIT extends AbstractCommandIT
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;

    private static Stream<Arguments> upgradableDatabases()
    {
        return Stream.of(
                Arguments.of( "standardEmpty34", "0.A.9-empty.zip", 0, Standard.LATEST_RECORD_FORMATS, 0 ),
                Arguments.of( "standardData34", "0.A.9-data.zip", 174, Standard.LATEST_RECORD_FORMATS, 3 ),
                Arguments.of( "highLimitEmpty34", "E.H.4-empty.zip", 0, HighLimit.RECORD_FORMATS, 0 ),
                Arguments.of( "highLimitData34", "E.H.4-data.zip", 174, HighLimit.RECORD_FORMATS, 3 ),
                Arguments.of( "highLimitEmpty30", "E.H.0-empty.zip", 0, HighLimit.RECORD_FORMATS, 0 ),
                Arguments.of( "highLimitData30", "E.H.0-data.zip", 174, HighLimit.RECORD_FORMATS, 3 ),
                Arguments.of( "standardEmpty40", "0.0.4FS-empty.zip", 0, Standard.LATEST_RECORD_FORMATS, 0 ),
                Arguments.of( "standardData40", "0.0.4FS-data.zip", 174, Standard.LATEST_RECORD_FORMATS, 6 )
        );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "upgradableDatabases" )
    void upgradeAndStartDatabase( String database, String sourceResource, long expectedNumberOfNodes, RecordFormats expectedRecordFormat,
            long expectedNumberOfSchema ) throws Exception
    {
        // Prepare old database
        Path source = testDirectory.directory( "source" );
        Unzip.unzip( StoreUpgradeIT.class, sourceResource, source );

        // Execute copy command
        String target = database + "upgraded";
        copyDatabase(
                "--from-path=" + source.toAbsolutePath(),
                "--to-database=" + target,
                "--to-format=same" );

        // Startup database in the DBMS and verify data
        managementService.createDatabase( target );
        GraphDatabaseAPI copyDb = (GraphDatabaseAPI) managementService.database( target );
        try ( Transaction tx = copyDb.beginTx() )
        {
            assertEquals( expectedNumberOfNodes, tx.getAllNodes().iterator().stream().count() );
            tx.commit();
        }

        assertEquals( expectedRecordFormat, selectForStore( copyDb.databaseLayout(), fs, pageCache, getInstance(), NULL ) );

        // Read out the schema statements
        String output = out.toString();
        int found = output.indexOf( format( "found %d schema definitions", expectedNumberOfSchema ) );
        assertThat( found ).isGreaterThanOrEqualTo( 0 );
        if ( expectedNumberOfSchema > 0 )
        {
            Scanner scanner = new Scanner( output.substring( output.indexOf( "CALL ", found ) ) );
            scanner.useDelimiter( System.lineSeparator() );
            List<String> createStatements = new ArrayList<>();
            while ( scanner.hasNext() )
            {
                String line = scanner.next();
                if ( !line.startsWith( "CALL " ) )
                {
                    break;
                }
                createStatements.add( line );
            }

            assertThat( createStatements ).hasSize( (int) expectedNumberOfSchema );

            // Recreate schemas
            try ( Transaction tx = copyDb.beginTx() )
            {
                for ( String createStatement : createStatements )
                {
                    tx.execute( createStatement ).close();
                }
                tx.commit();
            }

            // Wait until they are populated
            try ( Transaction tx = copyDb.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
                tx.commit();
            }

            // Make sure everyone made it
            Map<String,Map<String,Object>> indexNameToSchemaStatements = new HashMap<>();
            try ( Transaction tx = copyDb.beginTx();
                    Result result = tx.execute( "CALL db.schemaStatements()" ) )
            {
                while ( result.hasNext() )
                {
                    final Map<String,Object> next = result.next();
                    indexNameToSchemaStatements.put( (String) next.get( "name" ), next );
                }
                tx.commit();
            }
            assertThat( indexNameToSchemaStatements ).hasSize( (int) expectedNumberOfSchema );
        }
    }

    private void copyDatabase( String... args ) throws Exception
    {
        var command = new StoreCopyCommand( getExtensionContext() );

        CommandLine.populateCommand( command, args );
        command.setPageCacheTracer( NULL );
        command.execute();
    }
}
