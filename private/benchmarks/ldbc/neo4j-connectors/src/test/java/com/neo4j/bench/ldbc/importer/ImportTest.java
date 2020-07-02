/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.cli.ImportCommand;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.LdbcDateCodecUtil;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
class ImportTest
{
    @Inject
    private TestDirectory temporaryFolder;

    @Inject
    private RandomRule randomRule;

    private boolean withUnique()
    {
        return Math.abs( randomRule.nextInt() ) % 2 == 0;
    }

    private boolean withMandatory()
    {
        return Math.abs( randomRule.nextInt() ) % 2 == 0;
    }

    @Test
    void shouldLoadDatasetUsingDefaultImporterWithoutMandatoryConstraints() throws Exception
    {
        doShouldLoadDatasetUsingDefaultImporter( false );
    }

    @Test
    void shouldLoadDatasetUsingDefaultImporterWithMandatoryConstraints() throws Exception
    {
        doShouldLoadDatasetUsingDefaultImporter( true );
    }

    private void doShouldLoadDatasetUsingDefaultImporter( boolean withMandatory ) throws Exception
    {
        boolean withUnique = withUnique();
        Scenario scenario = Scenario.randomInteractive();
        File storeDir = temporaryFolder.directory( "store" );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        String[] args = new String[]{
                "import",
                ImportCommand.CMD_CSV_SCHEMA, scenario.csvSchema().name(),
                ImportCommand.CMD_NEO4J_SCHEMA, scenario.neo4jSchema().name(),
                ImportCommand.CMD_DB, storeDir.getAbsolutePath(),
                ImportCommand.CMD_CSV, scenario.csvDir().getAbsolutePath(),
                ImportCommand.CMD_SOURCE_DATE, scenario.csvDateFormat().name(),
                ImportCommand.CMD_TARGET_DATE, scenario.neo4jDateFormat().name(),
                ImportCommand.CMD_TIMESTAMP_RESOLUTION, scenario.timestampResolution().name(),
                ImportCommand.CMD_CONFIG, configFile.getAbsolutePath()
        };
        if ( withUnique )
        {
            args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_UNIQUE );
        }
        if ( withMandatory )
        {
            args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_MANDATORY );
        }
        LdbcCli.main( args );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                scenario.neo4jSchema(),
                scenario.neo4jDateFormat(),
                scenario.timestampResolution() );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldThrowExceptionWhenUsingDefaultImporterAndMissingDbDir() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        Scenario scenario = Scenario.randomInteractive();
        File config = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );

        assertThrows( RuntimeException.class, () ->
        {
            String[] args = new String[]{
                    "import",
                    ImportCommand.CMD_CSV, scenario.csvDir().getAbsolutePath(),
                    ImportCommand.CMD_SOURCE_DATE, scenario.csvDateFormat().name(),
                    ImportCommand.CMD_TARGET_DATE, scenario.neo4jDateFormat().name(),
                    ImportCommand.CMD_CONFIG, config.getAbsolutePath()
            };
            if ( withUnique )
            {
                args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_UNIQUE );
            }
            if ( withMandatory )
            {
                args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_MANDATORY );
            }
            LdbcCli.main( args );
        } );
    }

    @Test
    void shouldThrowExceptionWhenUsingDefaultImporterAndMissingCsvDir() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        Scenario scenario = Scenario.randomInteractive();
        File storeDir = temporaryFolder.directory( "store" );
        assertThrows( RuntimeException.class, () ->
        {
            File config = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
            String[] args = new String[]{
                    "import",
                    ImportCommand.CMD_DB, storeDir.getAbsolutePath(),
                    ImportCommand.CMD_SOURCE_DATE, scenario.csvDateFormat().name(),
                    ImportCommand.CMD_TARGET_DATE, scenario.neo4jDateFormat().name(),
                    ImportCommand.CMD_CONFIG, config.getAbsolutePath()
            };
            if ( withUnique )
            {
                args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_UNIQUE );
            }
            if ( withMandatory )
            {
                args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_MANDATORY );
            }
            LdbcCli.main( args );
        } );
    }

    @Test
    void shouldThrowExceptionWhenUsingDefaultImporterAndMissingIsLongDateFlag() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        Scenario scenario = Scenario.randomInteractive();
        File storeDir = temporaryFolder.directory( "store" );
        assertThrows( RuntimeException.class, () ->
        {
            File config = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
            String[] args = new String[]{
                    "import",
                    ImportCommand.CMD_DB, storeDir.getAbsolutePath(),
                    ImportCommand.CMD_CSV, scenario.csvDir().getAbsolutePath(),
                    ImportCommand.CMD_CONFIG, config.getAbsolutePath()
            };
            if ( withUnique )
            {
                args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_UNIQUE );
            }
            if ( withMandatory )
            {
                args = Utils.copyArrayAndAddElement( args, ImportCommand.CMD_WITH_MANDATORY );
            }
            LdbcCli.main( args );
            LdbcCli.index(
                    storeDir,
                    config,
                    scenario.neo4jSchema(),
                    withUnique,
                    withMandatory,
                    true );
        } );
    }

    @Test
    void shouldImportUsingParallelForRegularWithCsvStringDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/string_date/" );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelRegular(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldImportUsingParallelForRegularWithCsvStringDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/string_date/" );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelRegular(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldImportUsingParallelForRegularWithCsvUtcDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/num_date/" );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelRegular(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_UTC );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldImportUsingParallelForRegularWithCsvUtcDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/num_date/" );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelRegular(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_ENCODED );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldImportUsingParallelForDense1WithCsvStringDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/string_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelImportDense1(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldImportUsingParallelForDense1WithCsvStringDateNeo4jEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/string_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelImportDense1(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldImportUsingParallelForDense1WithCsvUtcDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/num_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelImportDense1(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );

        assertConsistentStore( storeDir );
    }

    @Test
    void shouldImportUsingParallelForDense1WithCsvUtcDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File storeDir = temporaryFolder.directory( "store" );
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/num_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcCli.importParallelImportDense1(
                storeDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );
        LdbcCli.inspect(
                storeDir,
                configFile );
        LdbcCli.index(
                storeDir,
                configFile,
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                storeDir,
                configFile );

        assertGraphMetadataIsAsExpected(
                storeDir,
                configFile,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );

        assertConsistentStore( storeDir );
    }

    private void assertGraphMetadataIsAsExpected(
            File dbDir,
            File configFile,
            Neo4jSchema neo4jSchema,
            LdbcDateCodec.Format neo4jFormat,
            LdbcDateCodec.Resolution timestampResolution ) throws DbException
    {
        DatabaseManagementService managementService = Neo4jDb.newDb( dbDir, configFile );
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        GraphMetadataProxy metadata = GraphMetadataProxy.loadFrom( db );
        QueryDateUtil dateUtil = QueryDateUtil.createFor( neo4jFormat, timestampResolution, new LdbcDateCodecUtil() );

        if ( metadata.hasCommentHasCreatorMinDateAtResolution() )
        {
            assertThat(
                    metadata.commentHasCreatorMinDateAtResolution(),
                    equalTo( dateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( 20100110010000000L ) ) );
        }
        if ( metadata.hasCommentHasCreatorMaxDateAtResolution() )
        {
            assertThat(
                    metadata.commentHasCreatorMaxDateAtResolution(),
                    equalTo( dateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( 20101125110000000L ) ) );
        }
        if ( metadata.hasPostHasCreatorMinDateAtResolution() )
        {
            assertThat(
                    metadata.postHasCreatorMinDateAtResolution(),
                    equalTo( dateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( 20100103050000000L ) ) );
        }
        if ( metadata.hasPostHasCreatorMaxDateAtResolution() )
        {
            assertThat(
                    metadata.postHasCreatorMaxDateAtResolution(),
                    equalTo( dateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( 20101125110000000L ) ) );
        }
        assertThat(
                metadata.hasWorkFromMinYear(),
                equalTo( !neo4jSchema.equals( Neo4jSchema.NEO4J_REGULAR ) ) );
        if ( metadata.hasWorkFromMinYear() )
        {
            assertThat(
                    metadata.workFromMinYear(),
                    equalTo( 1998 ) );
        }
        assertThat(
                metadata.hasWorkFromMaxYear(),
                equalTo( !neo4jSchema.equals( Neo4jSchema.NEO4J_REGULAR ) ) );
        if ( metadata.hasWorkFromMaxYear() )
        {
            assertThat(
                    metadata.workFromMaxYear(),
                    equalTo( 2012 ) );
        }
        assertThat(
                metadata.dateFormat(),
                equalTo( neo4jFormat ) );
        assertThat(
                metadata.timestampResolution(),
                equalTo( timestampResolution ) );
        assertThat(
                metadata.neo4jSchema(),
                equalTo( neo4jSchema ) );

        managementService.shutdown();
    }

    private void assertConsistentStore( File storeDir ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService.Result result = new ConsistencyCheckService( new Date() )
                .runFullConsistencyCheck(
                        Neo4jDb.layoutWithTxLogLocation( storeDir ),
                        Config.defaults(),
                        ProgressMonitorFactory.NONE,
                        NullLogProvider.getInstance(),
                        false,
                        new ConsistencyFlags( true, true, true, true, true, true ) );
        if ( !result.isSuccessful() )
        {
            try
            {
                System.err.println( "Store " + storeDir + " not consistent:" );
                Files.lines( result.reportFile() ).forEach( System.err::println );
            }
            catch ( IOException e )
            {
                System.err.println( "Tried to read report file from unsuccessful consistency check at " + result.reportFile() + ", but failed" );
                e.printStackTrace();
            }
        }
        assertTrue( result.isSuccessful() );
    }
}
