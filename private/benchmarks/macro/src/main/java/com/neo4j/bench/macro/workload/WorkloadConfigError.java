/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

public enum WorkloadConfigError
{
    NO_QUERIES( "'queries' field is missing from config file." ),
    EMPTY_QUERIES( "'queries' field is present, but zero queries are specified." ),
    PARAM_FILE_NOT_FOUND( "Specified parameter file was not found." ),
    QUERY_FILE_NOT_FOUND( "Specified query file was not found." ),
    SCHEMA_FILE_NOT_FOUND( "Specified schema file was not found." ),
    NO_PARAM_FILE( "'parameters' field is present, but no parameter file was specified." ),
    NO_QUERY_FILE( "'queries' field is present, but a query has not specified its file." ),
    NO_QUERY_NAME( "'queries' field is present, but a query has not specified its name." ),
    NO_SCHEMA( "'schema' field is missing from config file." ),
    NO_WORKLOAD_NAME( "Workload name has not been specified." ),
    INVALID_QUERY_FIELD( "A query in 'queries' contained an unrecognized field." ),
    INVALID_WORKLOAD_FIELD( "Workload contained an unrecognized field." ),
    INVALID_SCHEMA_ENTRY( "Invalid entry found in workload schema definition." );

    private final String message;

    WorkloadConfigError( String message )
    {
        this.message = message;
    }

    public String message()
    {
        return message;
    }
}
