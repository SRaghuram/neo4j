/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

/**
 * An indication of a type of a statement and what types of statement might be coming later in the same transaction.
 */
public enum TransactionMode
{
    /**
     * The current statement is a read, but a write statement might be coming later.
     */
    MAYBE_WRITE,

    /**
     * The current statement is a write.
     */
    DEFINITELY_WRITE,

    /**
     * The current statement is a read and no write statement will follow.
     */
    DEFINITELY_READ
}
