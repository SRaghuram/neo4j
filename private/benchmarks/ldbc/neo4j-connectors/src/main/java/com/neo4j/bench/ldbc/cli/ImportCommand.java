/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

import static java.lang.String.format;

@Command(
        name = "import",
        description = "Imports LDBC CSV file into Neo4j" )
public class ImportCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( ImportCommand.class );

    public static final String CMD_CSV_SCHEMA = "--csv-schema";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CSV_SCHEMA},
            description = "Format of source CSV files: CSV_REGULAR, CSV_MERGE",
            title = "CSV Schema" )
    @Required
    private String csvSchemaString;

    public static final String CMD_NEO4J_SCHEMA = "--neo4j-schema";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_SCHEMA},
            description = "Desired schema of target Neo4j store: NEO4J_REGULAR, NEO4J_DENSE_1",
            title = "Neo4j Schema" )
    @Required
    private String neo4jSchemaString;

    public static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Target Neo4j database directory",
            title = "Database Directory" )
    @Required
    private File dbDir;

    public static final String CMD_CSV = "--csv";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CSV},
            description = "Source CSV dataset directory",
            title = "CSV Dir" )
    @Required
    private File csvDir;

    public static final String CMD_WITH_UNIQUE = "--with-unique";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WITH_UNIQUE},
             description = "Create unique constraints for properties that should have unique values",
             title = "Create Unique Constraints" )
    private boolean withUnique;

    public static final String CMD_WITH_MANDATORY = "--with-mandatory";
    @Option( type = OptionType.COMMAND,
             name = {CMD_WITH_MANDATORY},
             description = "Create mandatory constraints for required properties",
             title = "Create Mandatory Constraints" )
    private boolean withMandatory;

    public static final String CMD_SOURCE_DATE = "--source-date";
    @Option( type = OptionType.COMMAND,
            name = {CMD_SOURCE_DATE},
            description = "Format of date values in source CSV files: STRING_ENCODED, NUMBER_UTC, NUMBER_ENCODED",
            title = "Source Date Format" )
    @Required
    private LdbcDateCodec.Format fromCsvFormat = LdbcDateCodec.Format.NUMBER_ENCODED;

    public static final String CMD_TARGET_DATE = "--target-date";
    @Option( type = OptionType.COMMAND,
            name = {CMD_TARGET_DATE},
            description = "Format to store date in, in Neo4j database: STRING_ENCODED, NUMBER_UTC, NUMBER_ENCODED",
            title = "Target Date Format" )
    @Required
    private LdbcDateCodec.Format toNeo4JFormat = LdbcDateCodec.Format.NUMBER_ENCODED;

    public static final String CMD_TIMESTAMP_RESOLUTION = "--timestamp-resolution";
    @Option( type = OptionType.COMMAND,
             name = {CMD_TIMESTAMP_RESOLUTION},
             description = "Resolution of timestamp to append to 'dense' relationship types: " +
                           "NOT_APPLICABLE, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND",
             title = "Timestamp Resolution" )
    private LdbcDateCodec.Resolution timestampResolution = LdbcDateCodec.Resolution.NOT_APPLICABLE;

    public static final String CMD_CONFIG = "--config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CONFIG},
            description = "Import configuration file",
            title = "Importer Config" )
    private File importerConfigurationFile;

    @Override
    public void run()
    {
        LOG.debug( format( "Source CSV Schema              : %s",
                                    csvSchemaString ) );
        LOG.debug( format( "Source CSV Directory           : %s",
                                    (null == csvDir) ? null : csvDir.getAbsolutePath() ) );
        LOG.debug( format( "Target Neo4j Schema            : %s",
                                    neo4jSchemaString ) );
        LOG.debug( format( "Target Neo4j Directory         : %s",
                                    (null == dbDir) ? null : dbDir.getAbsolutePath() ) );
        LOG.debug( format( "Create Unique Constraints      : %s",
                                    withUnique ) );
        LOG.debug( format( "Create Mandatory Constraints   : %s",
                                    withMandatory ) );
        LOG.debug( format( "Source CSV Date Format         : %s",
                                    fromCsvFormat ) );
        LOG.debug( format( "Target Neo4j Date Format       : %s",
                                    toNeo4JFormat ) );
        LOG.debug( format( "Dense Timestamp Resolution     : %s",
                                    timestampResolution ) );
        LOG.debug( format( "Importer Configuration File    : %s",
                                    (null == importerConfigurationFile) ? null : importerConfigurationFile.getAbsolutePath() ) );

        LOG.debug( "*** Neo4j DB Properties ***" );
        LOG.debug( Neo4jDb.configToString( importerConfigurationFile ) );
        LOG.debug( "************************" );

        if ( !csvDir.exists() )
        {
            throw new RuntimeException( format( "Source CSV directory not found: %s", csvDir.getAbsolutePath() ) );
        }

        CsvSchema csvSchema;
        try
        {
            csvSchema = CsvSchema.valueOf( csvSchemaString );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Invalid CSV Schema: %s\nValid values are: %s",
                                                csvSchemaString, Arrays.toString( CsvSchema.values() ) ) );
        }
        Neo4jSchema neo4jSchema;
        try
        {
            neo4jSchema = Neo4jSchema.valueOf( neo4jSchemaString );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Invalid Neo4j Schema: %s\nValid values are: %s",
                                                neo4jSchemaString, Arrays.toString( Neo4jSchema.values() ) ) );
        }

        LdbcSnbImporter importer = LdbcSnbImporter.importerFor( csvSchema, neo4jSchema );

        try
        {
            importer.load(
                    dbDir,
                    csvDir,
                    importerConfigurationFile,
                    fromCsvFormat,
                    toNeo4JFormat,
                    timestampResolution,
                    withUnique,
                    withMandatory
            );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Encountered error while importing data", e );
        }
    }

    public static String[] buildArgs(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            File dbDir,
            File csvDir,
            File importerConfigurationFile,
            boolean withUnique,
            boolean withMandatory,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat,
            LdbcDateCodec.Resolution timestampResolution )
    {
        String[] args = new String[]{
                "import",
                CMD_CSV_SCHEMA, csvSchema.name(),
                CMD_NEO4J_SCHEMA, neo4jSchema.name(),
                CMD_DB, dbDir.getAbsolutePath(),
                CMD_CSV, csvDir.getAbsolutePath(),
                CMD_SOURCE_DATE, fromCsvFormat.name(),
                CMD_TARGET_DATE, toNeo4JFormat.name(),
                CMD_TIMESTAMP_RESOLUTION, timestampResolution.name()
        };
        if ( null != importerConfigurationFile )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_CONFIG );
            args = Utils.copyArrayAndAddElement( args, importerConfigurationFile.getAbsolutePath() );
        }
        if ( withUnique )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_WITH_UNIQUE );
        }
        if ( withMandatory )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_WITH_MANDATORY );
        }
        return args;
    }
}
