/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

public enum CatchupResult
{
    SUCCESS_END_OF_STREAM,
    E_STORE_ID_MISMATCH,
    E_STORE_UNAVAILABLE,
    E_TRANSACTION_PRUNED,
    E_INVALID_REQUEST,
    E_DATABASE_UNKNOWN,
    E_GENERAL_ERROR
}
