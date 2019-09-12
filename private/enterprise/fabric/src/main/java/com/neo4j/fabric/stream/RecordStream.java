/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import com.neo4j.fabric.stream.summary.Summary;

import java.util.List;

public interface RecordStream extends AutoCloseable
{
    List<String> getColumns();

    Record readRecord();

    Summary summary();

    @Override
    void close();
}
