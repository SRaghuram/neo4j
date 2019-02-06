/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.google.common.base.Charsets;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.neo4j.bench.ldbc.cli.RunCommand.LdbcRunConfig;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jImporter;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static com.ldbc.driver.control.ConsoleAndFileDriverConfiguration.fromParamsMap;
import static com.ldbc.driver.util.MapUtils.loadPropertiesToMap;

public class LdbcCli
{
    public static final Charset CHARSET = Charsets.UTF_8;
    public static final Map<String,Object> EMPTY_MAP = new HashMap<>();

    public static void main( String[] args ) throws Exception
    {
        Cli.<Runnable>builder( "ldbc" )
                .withDefaultCommand( Help.class )
                .withCommand( ImportCommand.class )
                .withCommand( IndexCommand.class )
                .withCommand( InspectCommand.class )
                .withCommand( RunExportCommand.class )
                .withCommand( RunCommand.class )
                .withCommand( UpgradeStoreCommand.class )
                .withCommand( Help.class )
                .build()
                .parse( args )
                .run();
    }

    public static void importBatchRegular(
            File dbDir,
            File csvDataDir,
            File importerPropertiesFile,
            boolean createUniqueConstraints,
            boolean createMandatoryConstraints,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat ) throws Exception
    {
        main(
                ImportCommand.buildArgs(
                        Neo4jImporter.BATCH,
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        dbDir,
                        csvDataDir,
                        importerPropertiesFile,
                        createUniqueConstraints,
                        createMandatoryConstraints,
                        fromCsvFormat,
                        toNeo4JFormat,
                        LdbcDateCodec.Resolution.NOT_APPLICABLE
                )
        );
    }

    public static void importBatchDense1(
            File dbDir,
            File csvDataDir,
            File importerPropertiesFile,
            boolean createUniqueConstraints,
            boolean createMandatoryConstraints,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat,
            LdbcDateCodec.Resolution timestampResolution ) throws Exception
    {
        main(
                ImportCommand.buildArgs(
                        Neo4jImporter.BATCH,
                        CsvSchema.CSV_MERGE,
                        Neo4jSchema.NEO4J_DENSE_1,
                        dbDir,
                        csvDataDir,
                        importerPropertiesFile,
                        createUniqueConstraints,
                        createMandatoryConstraints,
                        fromCsvFormat,
                        toNeo4JFormat,
                        timestampResolution
                )
        );
    }

    public static void importParallelRegular(
            File dbDir,
            File csvDataDir,
            boolean createUniqueConstraints,
            boolean createMandatoryConstraints,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat ) throws Exception
    {
        main(
                ImportCommand.buildArgs(
                        Neo4jImporter.PARALLEL,
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        dbDir,
                        csvDataDir,
                        null,
                        createUniqueConstraints,
                        createMandatoryConstraints,
                        fromCsvFormat,
                        toNeo4JFormat,
                        LdbcDateCodec.Resolution.NOT_APPLICABLE
                )
        );
    }

    public static void importParallelImportDense1(
            File dbDir,
            File csvDataDir,
            boolean createUniqueConstraints,
            boolean createMandatoryConstraints,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat,
            LdbcDateCodec.Resolution timestampResolution ) throws Exception
    {
        main(
                ImportCommand.buildArgs(
                        Neo4jImporter.PARALLEL,
                        CsvSchema.CSV_MERGE,
                        Neo4jSchema.NEO4J_DENSE_1,
                        dbDir,
                        csvDataDir,
                        null,
                        createUniqueConstraints,
                        createMandatoryConstraints,
                        fromCsvFormat,
                        toNeo4JFormat,
                        timestampResolution
                )
        );
    }

    public static void index(
            File dbDir,
            File dbConfigurationFile,
            Neo4jSchema neo4jSchema,
            boolean withUnique,
            boolean withMandatory,
            boolean dropFirst ) throws Exception
    {
        main(
                IndexCommand.buildArgs(
                        dbDir,
                        dbConfigurationFile,
                        neo4jSchema,
                        withUnique,
                        withMandatory,
                        dropFirst
                )
        );
    }

    public static void inspect(
            File dbDir,
            File dbConfigurationFile ) throws Exception
    {
        main(
                InspectCommand.buildArgs(
                        dbDir,
                        dbConfigurationFile
                )
        );
    }

    public static void benchmark(
            File dbDir,
            File writeParametersDir,
            File readParametersDir,
            File resultsDir,
            Neo4jApi neo4jApi,
            File ldbcConfig,
            File neo4jConfig,
            int readThreads ) throws Exception
    {
        ConsoleAndFileDriverConfiguration ldbcDriverConfig = fromParamsMap( loadPropertiesToMap( ldbcConfig ) );
        main(
                RunCommand.buildArgs(
                        new LdbcRunConfig(
                                dbDir,
                                writeParametersDir,
                                readParametersDir,
                                neo4jApi,
                                PlannerType.valueOf( ldbcDriverConfig.asMap().getOrDefault( "neo4j.planner", PlannerType.DEFAULT.name() ) ),
                                RuntimeType.valueOf( ldbcDriverConfig.asMap().getOrDefault( "neo4j.runtime", RuntimeType.DEFAULT.name() ) ),
                                ldbcConfig,
                                neo4jConfig,
                                readThreads,
                                ldbcDriverConfig.warmupCount(),
                                ldbcDriverConfig.operationCount(),
                                null
                        ),
                        resultsDir
                )
        );
    }
}
