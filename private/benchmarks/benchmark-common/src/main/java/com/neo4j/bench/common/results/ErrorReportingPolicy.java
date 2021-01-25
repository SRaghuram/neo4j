/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

public enum ErrorReportingPolicy
{
    // report regardless of errors. exit cleanly regardless of errors.
    IGNORE,
    // report regardless of errors. exit with error if report contains errors.
    REPORT_THEN_FAIL,
    // only report if there are no errors. exit with error if report contains errors.
    FAIL
}
