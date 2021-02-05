/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import org.neo4j.annotations.api.PublicApi;

@PublicApi
public enum TransactionStreamingStrategy
{
    Unbounded,
    StartTime
}
