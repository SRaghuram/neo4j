/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.dense1.LdbcSnbImporterParallelDense1;
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
            Neo4jSchema neo4jSchema )
    {

        switch ( csvSchema )
        {
        case CSV_REGULAR:
            switch ( neo4jSchema )
            {
            case NEO4J_REGULAR:
                // Simple CSV, Regular Schema
                return new LdbcSnbImporterParallelRegular();
            case NEO4J_DENSE_1:
                // Simple CSV, Dense 1 Schema
                throw new RuntimeException( getUnsupportedCombinationExceptionMessage( csvSchema, neo4jSchema ) );
            default:
                throw new RuntimeException( getUnsupportedCombinationExceptionMessage( csvSchema, neo4jSchema ) );
            }
        case CSV_MERGE:
            switch ( neo4jSchema )
            {
            case NEO4J_REGULAR:
                // Merge CSV, Regular Schema
                throw new RuntimeException( getUnsupportedCombinationExceptionMessage( csvSchema, neo4jSchema ) );
            case NEO4J_DENSE_1:
                // Merge CSV, Dense 1 Schema, Parallel
                return new LdbcSnbImporterParallelDense1();
            default:
                throw new RuntimeException( getUnsupportedCombinationExceptionMessage( csvSchema, neo4jSchema ) );
            }
        default:
            throw new RuntimeException( getUnsupportedCombinationExceptionMessage( csvSchema, neo4jSchema ) );
        }
    }

    private static String getUnsupportedCombinationExceptionMessage( CsvSchema csvSchema, Neo4jSchema neo4jSchema )
    {
        return format( "Unsupported Combination: %s / %s", csvSchema, neo4jSchema );
    }
}
