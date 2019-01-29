/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jImporter;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;

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
    private final Neo4jImporter neo4jImporter;
    private final Neo4jApi neo4JApi;
    private final PlannerType planner;
    private final RuntimeType runtime;
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
            Neo4jImporter neo4jImporter,
            Neo4jApi neo4JApi,
            PlannerType planner,
            RuntimeType runtime,
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
        this.neo4jImporter = neo4jImporter;
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

    public Neo4jImporter neo4jImporter()
    {
        return neo4jImporter;
    }

    public Neo4jApi neo4jApi()
    {
        return neo4JApi;
    }

    public PlannerType planner()
    {
        return planner;
    }

    public RuntimeType runtime()
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
               "   neo4jImporter=" + neo4jImporter.name() + "\n" +
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
                    format( "Unsupported workload: %s", workloadClass.getClass().getSimpleName() ) );
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

    public static Neo4jImporter randomNeo4jImporter()
    {
        return (Math.abs( RANDOM.nextInt() ) % 2 == 0) ? Neo4jImporter.BATCH : Neo4jImporter.PARALLEL;
    }

    public static Neo4jImporter randomNeo4jImporterFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema )
    {
        switch ( csvSchema )
        {
        case CSV_REGULAR:
            switch ( neo4jSchema )
            {
            case NEO4J_REGULAR:
                return randomNeo4jImporter();
            case NEO4J_DENSE_1:
                throw new RuntimeException( format( "Unsupported Combination: %s / %s", csvSchema, neo4jSchema ) );
            default:
                throw new RuntimeException( "unsupported Neo4j schema" );
            }
        case CSV_MERGE:
            switch ( neo4jSchema )
            {
            case NEO4J_REGULAR:
                throw new RuntimeException( format( "Unsupported Combination: %s / %s", csvSchema, neo4jSchema ) );
            case NEO4J_DENSE_1:
                return randomNeo4jImporter();
            default:
                throw new RuntimeException( "unsupported Neo4j schema" );
            }
        default:
            throw new RuntimeException( format( "Unsupported Combination: %s / %s", csvSchema, neo4jSchema ) );
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
            else if ( 1 == random % 2 )
            {
                return Neo4jApi.EMBEDDED_CYPHER;
            }
            else
            {
                throw new RuntimeException( format( "Unexpected random number: %s", random % 2 ) );
            }
        case NEO4J_DENSE_1:
            return Neo4jApi.EMBEDDED_CORE;
        default:
            throw new RuntimeException( format( "Unsupported Neo4j Schema: %s", neo4jSchema ) );
        }
    }

    public static PlannerType randomInteractivePlannerFor( Neo4jApi neo4jApi )
    {
        switch ( neo4jApi )
        {
        case EMBEDDED_CORE:
            return PlannerType.DEFAULT;
        case EMBEDDED_CYPHER:
        case REMOTE_CYPHER:
            int random = Math.abs( RANDOM.nextInt() );
            int plannerTypeIndex = random % PlannerType.values().length;
            return PlannerType.values()[plannerTypeIndex];
        default:
            throw new RuntimeException( format( "Unsupported Neo4j API: %s", neo4jApi ) );
        }
    }

    public static RuntimeType randomInteractiveRuntimeFor( Neo4jApi neo4jApi )
    {
        switch ( neo4jApi )
        {
        case EMBEDDED_CORE:
            return RuntimeType.DEFAULT;
        case EMBEDDED_CYPHER:
        case REMOTE_CYPHER:
            int random = Math.abs( RANDOM.nextInt() );
            int runtimeTypeIndex = random % RuntimeType.values().length;
            return RuntimeType.values()[runtimeTypeIndex];
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
        return randomInteractiveFor( csvSchema, neo4jSchema, randomNeo4jImporterFor( csvSchema, neo4jSchema ) );
    }

    public static Scenario randomInteractiveFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jImporter neo4jImporter ) throws DbException
    {
        Neo4jApi neo4jApi = randomInteractiveNeo4jConnectorFor( neo4jSchema );
        return randomInteractiveFor(
                csvSchema,
                neo4jSchema,
                neo4jImporter,
                neo4jApi,
                randomInteractivePlannerFor( neo4jApi ),
                randomInteractiveRuntimeFor( neo4jApi ) );
    }

    public static Scenario randomInteractiveFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jImporter neo4jImporter,
            Neo4jApi neo4JApi,
            PlannerType planner,
            RuntimeType runtime ) throws DbException
    {
        return randomInteractiveFor(
                csvSchema,
                neo4jSchema,
                neo4jImporter,
                neo4JApi,
                planner,
                runtime,
                randomCsvDateFormat(),
                randomNeo4jDateFormat() );
    }

    public static Scenario randomInteractiveFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jImporter neo4jImporter,
            Neo4jApi neo4JApi,
            PlannerType planner,
            RuntimeType runtime,
            LdbcDateCodec.Format csvFormat,
            LdbcDateCodec.Format neo4JFormat ) throws DbException
    {
        return randomInteractiveFor(
                csvSchema,
                neo4jSchema,
                neo4jImporter,
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
            Neo4jImporter neo4jImporter,
            Neo4jApi neo4JApi,
            PlannerType planner,
            RuntimeType runtime,
            LdbcDateCodec.Format csvFormat,
            LdbcDateCodec.Format neo4JFormat,
            LdbcDateCodec.Resolution timestampResolution ) throws DbException
    {
        return new Scenario(
                csvDirFor( csvFormat, neo4jSchema, LdbcSnbInteractiveWorkload.class ),
                PARAMS_DIR,
                UPDATES_DIR,
                INTERACTIVE_VALIDATION_SET,
                csvSchema,
                neo4jSchema,
                neo4jImporter,
                neo4JApi,
                planner,
                runtime,
                csvFormat,
                neo4JFormat,
                timestampResolution );
    }

    public static Scenario randomBi() throws DbException
    {
        CsvSchema csvSchema = CsvSchema.CSV_REGULAR;
        Neo4jSchema neo4jSchema = Neo4jSchema.NEO4J_REGULAR;
        Neo4jImporter neo4jImporter = randomNeo4jImporterFor( csvSchema, neo4jSchema );
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
                neo4jImporter,
                neo4JApi,
                PlannerType.DEFAULT,
                RuntimeType.DEFAULT,
                csvFormat,
                neo4JFormat,
                LdbcDateCodec.Resolution.NOT_APPLICABLE );
    }
}
