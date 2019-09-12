/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import java.util.List;

import org.neo4j.cypher.internal.runtime.InputDataStream;

public class RemoteDataStream
{
    private final List<String> columns;
    private final InputDataStream inputDataStream;

    public RemoteDataStream( List<String> columns, InputDataStream inputDataStream )
    {
        this.columns = columns;
        this.inputDataStream = inputDataStream;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public InputDataStream getInputDataStream()
    {
        return inputDataStream;
    }
}
