/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.report;

import org.apache.commons.lang3.StringEscapeUtils;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;

public abstract class CsvRow
{
    public final String row()
    {
        return Arrays.stream( unescapedRow() )
                     .map( StringEscapeUtils::escapeCsv )
                     .collect( joining( "," ) );
    }

    protected abstract String[] unescapedRow();
}
