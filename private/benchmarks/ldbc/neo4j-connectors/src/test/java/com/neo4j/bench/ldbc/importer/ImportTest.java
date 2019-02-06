/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ImportTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final Random RANDOM = new Random( System.currentTimeMillis() );

    private static boolean withUnique()
    {
        return Math.abs( RANDOM.nextInt() ) % 2 == 0;
    }

    private static boolean withMandatory()
    {
        return Math.abs( RANDOM.nextInt() ) % 2 == 0;
    }

    @Test
    public void shouldLoadDatasetUsingDefaultImporterWithoutMandatoryConstraints() throws Exception
    {
        doShouldLoadDatasetUsingDefaultImporter( false );
    }

    @Test
    public void shouldLoadDatasetUsingDefaultImporterWithMandatoryConstraints() throws Exception
    {
        doShouldLoadDatasetUsingDefaultImporter( true );
    }

    private void doShouldLoadDatasetUsingDefaultImporter( boolean withMandatory ) throws Exception
    {
        boolean withUnique = withUnique();
        Scenario scenario = Scenario.randomInteractive();
        File dbDir = temporaryFolder.newFolder();
        String[] args = new String[]{
                "import",
                ImportCommand.CMD_CSV_SCHEMA, scenario.csvSchema().name(),
                ImportCommand.CMD_NEO4J_SCHEMA, scenario.neo4jSchema().name(),
                ImportCommand.CMD_DB, dbDir.getAbsolutePath(),
                ImportCommand.CMD_CSV, scenario.csvDir().getAbsolutePath(),
                ImportCommand.CMD_SOURCE_DATE, scenario.csvDateFormat().name(),
                ImportCommand.CMD_TARGET_DATE, scenario.neo4jDateFormat().name(),
                ImportCommand.CMD_TIMESTAMP_RESOLUTION, scenario.timestampResolution().name(),
                ImportCommand.CMD_CONFIG, DriverConfigUtils.neo4jTestConfig().getAbsolutePath()
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
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                scenario.neo4jSchema(),
                scenario.neo4jDateFormat(),
                scenario.timestampResolution() );

        assertConsistentStore( dbDir );
    }

    @Test( expected = RuntimeException.class )
    public void shouldThrowExceptionWhenUsingDefaultImporterAndMissingDbDir() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        Scenario scenario = Scenario.randomInteractive();
        String[] args = new String[]{
                "import",
                ImportCommand.CMD_CSV, scenario.csvDir().getAbsolutePath(),
                ImportCommand.CMD_SOURCE_DATE, scenario.csvDateFormat().name(),
                ImportCommand.CMD_TARGET_DATE, scenario.neo4jDateFormat().name(),
                ImportCommand.CMD_CONFIG, DriverConfigUtils.neo4jTestConfig().getAbsolutePath()
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
    }

    @Test( expected = RuntimeException.class )
    public void shouldThrowExceptionWhenUsingDefaultImporterAndMissingCsvDir() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        Scenario scenario = Scenario.randomInteractive();
        File dbDir = temporaryFolder.newFolder();
        String[] args = new String[]{
                "import",
                ImportCommand.CMD_DB, dbDir.getAbsolutePath(),
                ImportCommand.CMD_SOURCE_DATE, scenario.csvDateFormat().name(),
                ImportCommand.CMD_TARGET_DATE, scenario.neo4jDateFormat().name(),
                ImportCommand.CMD_CONFIG, DriverConfigUtils.neo4jTestConfig().getAbsolutePath()
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
    }

    @Test( expected = RuntimeException.class )
    public void shouldThrowExceptionWhenUsingDefaultImporterAndMissingIsLongDateFlag() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        Scenario scenario = Scenario.randomInteractive();
        File dbDir = temporaryFolder.newFolder();
        String[] args = new String[]{
                "import",
                ImportCommand.CMD_DB, dbDir.getAbsolutePath(),
                ImportCommand.CMD_CSV, scenario.csvDir().getAbsolutePath(),
                ImportCommand.CMD_CONFIG, DriverConfigUtils.neo4jTestConfig().getAbsolutePath()
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
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                scenario.neo4jSchema(),
                withUnique,
                withMandatory,
                true );
    }

    @Test
    public void shouldImporterUsingBatchForRegularWithCsvStringDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/string_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcCli.importBatchRegular(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingBatchForRegularWithCsvStringDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/string_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcCli.importBatchRegular(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingBatchForRegularWithCsvUtcDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/num_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcCli.importBatchRegular(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_UTC );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingBatchForRegularWithCsvUtcDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/num_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcCli.importBatchRegular(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_ENCODED );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingBatchForDense1WithCsvStringDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/string_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importBatchDense1(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingBatchForDense1WithCsvStringDateNeo4jEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/string_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importBatchDense1(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingBatchForDense1WithCsvUtcDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/num_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importBatchDense1(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingBatchForDense1WithCsvUtcDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/num_date/" );
        File configFile =
                DriverConfigUtils.neo4jTestConfig();
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importBatchDense1(
                dbDir,
                csvFilesDir,
                configFile,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForRegularWithCsvStringDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/string_date/" );
        LdbcCli.importParallelRegular(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForRegularWithCsvStringDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/string_date/" );
        LdbcCli.importParallelRegular(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForRegularWithCsvUtcDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/num_date/" );
        LdbcCli.importParallelRegular(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_UTC );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForRegularWithCsvUtcDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/social_network/num_date/" );
        LdbcCli.importParallelRegular(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_ENCODED );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_REGULAR,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForDense1WithCsvStringDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/string_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importParallelImportDense1(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForDense1WithCsvStringDateNeo4jEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/string_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importParallelImportDense1(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForDense1WithCsvUtcDateNeo4jUtcDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/num_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importParallelImportDense1(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_UTC,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    @Test
    public void shouldImportUsingParallelForDense1WithCsvUtcDateNeo4jNumEncodedDate() throws Exception
    {
        boolean withUnique = withUnique();
        boolean withMandatory = withMandatory();
        File dbDir = temporaryFolder.newFolder();
        File csvFilesDir = DriverConfigUtils.getResource(
                "/validation_sets/data/merge/social_network/num_date/" );
        LdbcDateCodec.Resolution timestampResolution = Scenario.timestampResolution( Neo4jSchema.NEO4J_DENSE_1 );
        LdbcCli.importParallelImportDense1(
                dbDir,
                csvFilesDir,
                withUnique,
                withMandatory,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );
        LdbcCli.index(
                dbDir,
                DriverConfigUtils.neo4jTestConfig(),
                null,
                withUnique,
                withMandatory,
                true );
        LdbcCli.inspect(
                dbDir,
                DriverConfigUtils.neo4jTestConfig() );

        assertGraphMetadataIsAsExpected(
                dbDir,
                Neo4jSchema.NEO4J_DENSE_1,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                timestampResolution );

        assertConsistentStore( dbDir );
    }

    private void assertGraphMetadataIsAsExpected(
            File dbDir,
            Neo4jSchema neo4jSchema,
            LdbcDateCodec.Format neo4jFormat,
            LdbcDateCodec.Resolution timestampResolution ) throws DbException
    {
        GraphDatabaseService db = Neo4jDb.newDb( dbDir, DriverConfigUtils.neo4jTestConfig() );
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

        db.shutdown();
    }

    private void assertConsistentStore( File dbDir ) throws ConsistencyCheckIncompleteException, IOException
    {
        ConsistencyCheckService.Result result = new ConsistencyCheckService( new Date() )
                .runFullConsistencyCheck(
                        dbDir,
                        Config.defaults(),
                        ProgressMonitorFactory.NONE,
                        NullLogProvider.getInstance(),
                        false,
                        new ConsistencyFlags( true, false, true, true ) );
        assertTrue( result.isSuccessful() );
    }
}
