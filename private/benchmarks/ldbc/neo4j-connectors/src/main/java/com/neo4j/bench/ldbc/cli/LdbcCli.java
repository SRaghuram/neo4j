/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.help.Help;
import com.google.common.base.Charsets;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.ldbc.cli.RunCommand.LdbcRunConfig;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;

import java.io.File;
import java.nio.charset.Charset;

import static com.ldbc.driver.control.ConsoleAndFileDriverConfiguration.fromParamsMap;
import static com.ldbc.driver.util.MapUtils.loadPropertiesToMap;

public class LdbcCli
{
    public static final Charset CHARSET = Charsets.UTF_8;

    public static void main( String[] args )
    {
        Cli.<Runnable>builder( "ldbc" )
                .withDefaultCommand( Help.class )
                .withCommand( ImportCommand.class )
                .withCommand( IndexCommand.class )
                .withCommand( InspectCommand.class )
                .withCommand( RunReportCommand.class )
                .withCommand( RunCommand.class )
                .withCommand( UpgradeStoreCommand.class )
                .withCommand( Help.class )
                .build()
                .parse( args )
                .run();
    }

    public static void importParallelRegular(
            File dbDir,
            File csvDataDir,
            File configFile,
            boolean createUniqueConstraints,
            boolean createMandatoryConstraints,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat )
    {
        main(
                ImportCommand.buildArgs(
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        dbDir,
                        csvDataDir,
                        configFile,
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
            File configFile,
            boolean createUniqueConstraints,
            boolean createMandatoryConstraints,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat,
            LdbcDateCodec.Resolution timestampResolution )
    {
        main(
                ImportCommand.buildArgs(
                        CsvSchema.CSV_MERGE,
                        Neo4jSchema.NEO4J_DENSE_1,
                        dbDir,
                        csvDataDir,
                        configFile,
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
            boolean dropFirst )
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
            File dbConfigurationFile )
    {
        main(
                InspectCommand.buildArgs(
                        dbDir,
                        dbConfigurationFile
                )
        );
    }

    public static void benchmark(
            Store store,
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
                                store.topLevelDirectory().toFile(),
                                writeParametersDir,
                                readParametersDir,
                                neo4jApi,
                                Planner.valueOf( ldbcDriverConfig.asMap().getOrDefault( "neo4j.planner", Planner.DEFAULT.name() ) ),
                                Runtime.valueOf( ldbcDriverConfig.asMap().getOrDefault( "neo4j.runtime", Runtime.DEFAULT.name() ) ),
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
