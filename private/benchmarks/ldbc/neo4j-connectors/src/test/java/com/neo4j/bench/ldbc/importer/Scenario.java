/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;

import java.io.File;
import java.util.Random;

import static java.lang.String.format;

public class Scenario
{
    private static final File VALIDATION_SET_DIR = DriverConfigUtils.getResource(
            "/validation_sets/" );

    private static final File PARAMS_DIR = DriverConfigUtils.getResource(
            "/validation_sets/data/substitution_parameters" );
    private static final File UPDATES_DIR = DriverConfigUtils.getResource(
            "/validation_sets/data/updates/" );

    private static final File INTERACTIVE_VALIDATION_SET = DriverConfigUtils.getResource(
            "/validation_sets/neo4j/interactive/validation_params.csv" );
    private static final File BI_VALIDATION_SET = DriverConfigUtils.getResource(
            "/validation_sets/neo4j/business_intelligence/validation_params.csv" );

    private static final Random RANDOM = new Random();

    private final File csvDir;
    private final File paramsDir;
    private final File updatesDir;
    private final File validationParamsFile;
    private final CsvSchema csvSchema;
    private final Neo4jSchema neo4jSchema;
    private final Neo4jApi neo4JApi;
    private final Planner planner;
    private final Runtime runtime;
    private final LdbcDateCodec.Format csvFormat;
    private final LdbcDateCodec.Format neo4JFormat;
    private final LdbcDateCodec.Resolution timestampResolution;

    public Scenario(
            File csvDir,
            File paramsDir,
            File updatesDir,
            File validationParamsFile,
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jApi neo4JApi,
            Planner planner,
            Runtime runtime,
            LdbcDateCodec.Format csvFormat,
            LdbcDateCodec.Format neo4JFormat,
            LdbcDateCodec.Resolution timestampResolution )
    {
        this.csvDir = csvDir;
        this.paramsDir = paramsDir;
        this.updatesDir = updatesDir;
        this.validationParamsFile = validationParamsFile;
        this.csvSchema = csvSchema;
        this.neo4jSchema = neo4jSchema;
        this.neo4JApi = neo4JApi;
        this.planner = planner;
        this.runtime = runtime;
        this.csvFormat = csvFormat;
        this.neo4JFormat = neo4JFormat;
        this.timestampResolution = timestampResolution;
    }

    public File csvDir()
    {
        return csvDir;
    }

    public File paramsDir()
    {
        return paramsDir;
    }

    public File updatesDir()
    {
        return updatesDir;
    }

    public File validationParamsFile()
    {
        return validationParamsFile;
    }

    public CsvSchema csvSchema()
    {
        return csvSchema;
    }

    public Neo4jSchema neo4jSchema()
    {
        return neo4jSchema;
    }

    public Neo4jApi neo4jApi()
    {
        return neo4JApi;
    }

    public Planner planner()
    {
        return planner;
    }

    public Runtime runtime()
    {
        return runtime;
    }

    public LdbcDateCodec.Format csvDateFormat()
    {
        return csvFormat;
    }

    public LdbcDateCodec.Format neo4jDateFormat()
    {
        return neo4JFormat;
    }

    public LdbcDateCodec.Resolution timestampResolution()
    {
        return timestampResolution;
    }

    @Override
    public String toString()
    {
        return "Scenario{" + "\n" +
               "   csvDir=" + fileToString( csvDir ) + "\n" +
               "   paramsDir=" + fileToString( paramsDir ) + "\n" +
               "   updatesDir=" + fileToString( updatesDir ) + "\n" +
               "   validationParamsFile=" + fileToString( validationParamsFile ) + "\n" +
               "   csvSchema=" + csvSchema.name() + "\n" +
               "   neo4jSchema=" + neo4jSchema.name() + "\n" +
               "   neo4JApi=" + neo4JApi.name() + "\n" +
               "   planner=" + planner.name() + "\n" +
               "   runtime=" + runtime.name() + "\n" +
               "   csvFormat=" + csvFormat.name() + "\n" +
               "   neo4JFormat=" + neo4JFormat.name() + "\n" +
               "   timestampResolution=" + timestampResolution.name() + "\n" +
               '}';
    }

    private String fileToString( File file )
    {
        if ( null == file )
        {
            return null;
        }
        else
        {
            return file.getAbsolutePath();
        }
    }

    // =========== Static Util Methods ===========

    private static File csvDirFor(
            LdbcDateCodec.Format format,
            Neo4jSchema neo4jSchema,
            Class<? extends Workload> workloadClass )
    {
        if ( workloadClass.equals( LdbcSnbBiWorkload.class ) )
        {
            switch ( format )
            {
            case STRING_ENCODED:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                    return new File( VALIDATION_SET_DIR, "data/social_network/string_date/" );
                default:
                    throw new RuntimeException( format( "Unsupported schema : %s", neo4jSchema ) );
                }
            case NUMBER_UTC:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                    return new File( VALIDATION_SET_DIR, "data/social_network/num_date/" );
                default:
                    throw new RuntimeException( format( "Unsupported schema : %s", neo4jSchema ) );
                }
            case NUMBER_ENCODED:
                throw new RuntimeException( format( "Unsupported date format: %s", format ) );
            default:
                throw new RuntimeException( format( "Unsupported date format: %s", format ) );
            }
        }
        else if ( workloadClass.equals( LdbcSnbInteractiveWorkload.class ) )
        {
            switch ( format )
            {
            case STRING_ENCODED:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                    return new File( VALIDATION_SET_DIR, "data/social_network/string_date/" );
                case NEO4J_DENSE_1:
                    return new File( VALIDATION_SET_DIR, "data/merge/social_network/string_date/" );
                default:
                    throw new RuntimeException( format( "Unsupported schema : %s", neo4jSchema ) );
                }
            case NUMBER_UTC:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                    return new File( VALIDATION_SET_DIR, "data/social_network/num_date/" );
                case NEO4J_DENSE_1:
                    return new File( VALIDATION_SET_DIR, "data/merge/social_network/num_date/" );
                default:
                    throw new RuntimeException( format( "Unsupported schema : %s", neo4jSchema ) );
                }
            case NUMBER_ENCODED:
                throw new RuntimeException( format( "Unsupported date format: %s", format ) );
            default:
                throw new RuntimeException( format( "Unsupported date format: %s", format ) );
            }
        }
        else
        {
            throw new RuntimeException(
                    format( "Unsupported workload: %s", workloadClass.getSimpleName() ) );
        }
    }

    // =========== Static Random Methods ===========

    public static CsvSchema randomCsvSchema()
    {
        return (Math.abs( RANDOM.nextInt() ) % 2 == 0) ? CsvSchema.CSV_REGULAR : CsvSchema.CSV_MERGE;
    }

    public static Neo4jSchema randomInteractiveNeo4jSchemaFor( CsvSchema csvSchema )
    {
        switch ( csvSchema )
        {
        case CSV_REGULAR:
            return Neo4jSchema.NEO4J_REGULAR;
        case CSV_MERGE:
            return Neo4jSchema.NEO4J_DENSE_1;
        default:
            throw new RuntimeException( format( "Unsupported CSV Schema: %s", csvSchema ) );
        }
    }

    public static Neo4jApi randomInteractiveNeo4jConnectorFor( Neo4jSchema neo4jSchema )
    {
        switch ( neo4jSchema )
        {
        case NEO4J_REGULAR:
            int random = Math.abs( RANDOM.nextInt() );
            if ( 0 == random % 2 )
            {
                return Neo4jApi.EMBEDDED_CORE;
            }
            else
            {
                return Neo4jApi.EMBEDDED_CYPHER;
            }
        case NEO4J_DENSE_1:
            return Neo4jApi.EMBEDDED_CORE;
        default:
            throw new RuntimeException( format( "Unsupported Neo4j Schema: %s", neo4jSchema ) );
        }
    }

    public static Planner randomInteractivePlannerFor( Neo4jApi neo4jApi )
    {
        switch ( neo4jApi )
        {
        case EMBEDDED_CORE:
            return Planner.DEFAULT;
        case EMBEDDED_CYPHER:
        case REMOTE_CYPHER:
            int random = Math.abs( RANDOM.nextInt() );
            int plannerTypeIndex = random % Planner.values().length;
            return Planner.values()[plannerTypeIndex];
        default:
            throw new RuntimeException( format( "Unsupported Neo4j API: %s", neo4jApi ) );
        }
    }

    public static Runtime randomInteractiveRuntimeFor( Neo4jApi neo4jApi )
    {
        switch ( neo4jApi )
        {
        case EMBEDDED_CORE:
            return Runtime.DEFAULT;
        case EMBEDDED_CYPHER:
        case REMOTE_CYPHER:
            int random = Math.abs( RANDOM.nextInt() );
            int runtimeTypeIndex = random % Runtime.values().length;
            return Runtime.values()[runtimeTypeIndex];
        default:
            throw new RuntimeException( format( "Unsupported Neo4j API: %s", neo4jApi ) );
        }
    }

    public static LdbcDateCodec.Format randomCsvDateFormat()
    {
        // TODO expand with LdbcDateCodec.Format.NUMBER_ENCODED
        return (0 == Math.abs( RANDOM.nextInt() ) % 2)
               ? LdbcDateCodec.Format.NUMBER_UTC
               : LdbcDateCodec.Format.STRING_ENCODED;
    }

    public static LdbcDateCodec.Format randomNeo4jDateFormat()
    {
        return (0 == Math.abs( RANDOM.nextInt() ) % 2)
               ? LdbcDateCodec.Format.NUMBER_UTC
               : LdbcDateCodec.Format.NUMBER_ENCODED;
    }

    public static LdbcDateCodec.Resolution timestampResolution( Neo4jSchema neo4jSchema )
    {
        if ( neo4jSchema.equals( Neo4jSchema.NEO4J_REGULAR ) )
        {
            return LdbcDateCodec.Resolution.NOT_APPLICABLE;
        }
        else
        {
            // Note, returning anything finer-grained than HOUR results in massive date ranges and OOM exceptions
            int random = Math.abs( RANDOM.nextInt() );
            if ( 0 == random % 3 )
            {
                return LdbcDateCodec.Resolution.YEAR;
            }
            else if ( 1 == random % 3 )
            {
                return LdbcDateCodec.Resolution.MONTH;
            }
            else if ( 2 == random % 3 )
            {
                return LdbcDateCodec.Resolution.DAY;
            }
            else
            {
                throw new RuntimeException( format( "Unexpected modulus: %s", random % 3 ) );
            }
        }
    }

    public static Scenario randomInteractive() throws DbException
    {
        return randomInteractiveFor( randomCsvSchema() );
    }

    public static Scenario randomInteractiveFor( CsvSchema csvSchema ) throws DbException
    {
        return randomInteractiveFor( csvSchema, randomInteractiveNeo4jSchemaFor( csvSchema ) );
    }

    public static Scenario randomInteractiveFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema ) throws DbException
    {
        Neo4jApi neo4jApi = randomInteractiveNeo4jConnectorFor( neo4jSchema );
        return randomInteractiveFor(
                csvSchema,
                neo4jSchema,
                neo4jApi,
                randomInteractivePlannerFor( neo4jApi ),
                randomInteractiveRuntimeFor( neo4jApi ) );
    }

    public static Scenario randomInteractiveFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jApi neo4JApi,
            Planner planner,
            Runtime runtime )
    {
        return randomInteractiveFor(
                csvSchema,
                neo4jSchema,
                neo4JApi,
                planner,
                runtime,
                randomCsvDateFormat(),
                randomNeo4jDateFormat() );
    }

    public static Scenario randomInteractiveFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jApi neo4JApi,
            Planner planner,
            Runtime runtime,
            LdbcDateCodec.Format csvFormat,
            LdbcDateCodec.Format neo4JFormat )
    {
        return randomInteractiveFor(
                csvSchema,
                neo4jSchema,
                neo4JApi,
                planner,
                runtime,
                csvFormat,
                neo4JFormat,
                timestampResolution( neo4jSchema ) );
    }

    public static Scenario randomInteractiveFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jApi neo4JApi,
            Planner planner,
            Runtime runtime,
            LdbcDateCodec.Format csvFormat,
            LdbcDateCodec.Format neo4JFormat,
            LdbcDateCodec.Resolution timestampResolution )
    {
        return new Scenario(
                csvDirFor( csvFormat, neo4jSchema, LdbcSnbInteractiveWorkload.class ),
                PARAMS_DIR,
                UPDATES_DIR,
                INTERACTIVE_VALIDATION_SET,
                csvSchema,
                neo4jSchema,
                neo4JApi,
                planner,
                runtime,
                csvFormat,
                neo4JFormat,
                timestampResolution );
    }

    public static Scenario randomBi()
    {
        CsvSchema csvSchema = CsvSchema.CSV_REGULAR;
        Neo4jSchema neo4jSchema = Neo4jSchema.NEO4J_REGULAR;
        Neo4jApi neo4JApi = Neo4jApi.EMBEDDED_CYPHER;
        LdbcDateCodec.Format csvFormat = randomCsvDateFormat();
        LdbcDateCodec.Format neo4JFormat = LdbcDateCodec.Format.NUMBER_ENCODED;
        return new Scenario(
                csvDirFor( csvFormat, neo4jSchema, LdbcSnbBiWorkload.class ),
                PARAMS_DIR,
                UPDATES_DIR,
                BI_VALIDATION_SET,
                csvSchema,
                neo4jSchema,
                neo4JApi,
                Planner.DEFAULT,
                Runtime.DEFAULT,
                csvFormat,
                neo4JFormat,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );
    }
}
