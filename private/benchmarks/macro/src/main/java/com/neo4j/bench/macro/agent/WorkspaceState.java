/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.agent;

import java.nio.file.Path;

public class WorkspaceState
{
    private final Path product;
    private final Path dataset;

    public WorkspaceState( Path dataset, Path product )
    {
        this.product = product;
        this.dataset = dataset;
    }

    public Path product()
    {
        return product;
    }

    public Path dataset()
    {
        return dataset;
    }
}
