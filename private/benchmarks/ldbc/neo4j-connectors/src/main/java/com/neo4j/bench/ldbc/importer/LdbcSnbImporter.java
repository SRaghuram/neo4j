/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jImporter;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.dense1.LdbcSnbImporterBatchDense1;
import com.neo4j.bench.ldbc.importer.dense1.LdbcSnbImporterParallelDense1;
import com.neo4j.bench.ldbc.importer.regular.LdbcSnbImporterBatchRegular;
import com.neo4j.bench.ldbc.importer.regular.LdbcSnbImporterParallelRegular;

import java.io.File;

import static java.lang.String.format;

public abstract class LdbcSnbImporter
{
    public abstract void load(
            File dbDir,
            File csvDataDir,
            File importerProperties,
            LdbcDateCodec.Format fromCsvFormat,
            LdbcDateCodec.Format toNeo4JFormat,
            LdbcDateCodec.Resolution timestampResolution,
            boolean withUnique,
            boolean withMandatory ) throws Exception;

    public static LdbcSnbImporter importerFor(
            CsvSchema csvSchema,
            Neo4jSchema neo4jSchema,
            Neo4jImporter neo4jImporter )
    {

        switch ( csvSchema )
        {
        case CSV_REGULAR:
            switch ( neo4jSchema )
            {
            case NEO4J_REGULAR:
                switch ( neo4jImporter )
                {
                case BATCH:
                    // Simple CSV, Regular Schema, Batch
                    return new LdbcSnbImporterBatchRegular();
                case PARALLEL:
                    // Simple CSV, Regular Schema, Parallel
                    return new LdbcSnbImporterParallelRegular();
                default:
                    throw new RuntimeException( getUnsupportedCombinationExeptionMessage( csvSchema, neo4jSchema, neo4jImporter ) );
                }
            case NEO4J_DENSE_1:
                // Simple CSV, Dense 1 Schema
                throw new RuntimeException( getUnsupportedCombinationExeptionMessage( csvSchema, neo4jSchema, neo4jImporter ) );
            default:
                throw new RuntimeException( getUnsupportedCombinationExeptionMessage( csvSchema, neo4jSchema, neo4jImporter ) );
            }
        case CSV_MERGE:
            switch ( neo4jSchema )
            {
            case NEO4J_REGULAR:
                // Merge CSV, Regular Schema
                throw new RuntimeException( getUnsupportedCombinationExeptionMessage( csvSchema, neo4jSchema, neo4jImporter ) );
            case NEO4J_DENSE_1:
                switch ( neo4jImporter )
                {
                case BATCH:
                    // Merge CSV, Dense 1 Schema, Batch
                    return new LdbcSnbImporterBatchDense1();
                case PARALLEL:
                    // Merge CSV, Dense 1 Schema, Parallel
                    return new LdbcSnbImporterParallelDense1();
                default:
                    throw new RuntimeException( getUnsupportedCombinationExeptionMessage( csvSchema, neo4jSchema, neo4jImporter ) );
                }
            default:
                throw new RuntimeException( getUnsupportedCombinationExeptionMessage( csvSchema, neo4jSchema, neo4jImporter ) );
            }
        default:
            throw new RuntimeException( getUnsupportedCombinationExeptionMessage( csvSchema, neo4jSchema, neo4jImporter ) );
        }
    }

    private static String getUnsupportedCombinationExeptionMessage( CsvSchema csvSchema, Neo4jSchema neo4jSchema, Neo4jImporter neo4jImporter )
    {
        return format( "Unsupported Combination: %s / %s / %s",
                       csvSchema, neo4jSchema, neo4jImporter );
    }
}
