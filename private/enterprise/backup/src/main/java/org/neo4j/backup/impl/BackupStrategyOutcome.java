/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

/**
 * Possible states that can describe the outcome of any single backup strategy (BackupProtocol, TransactionProtocol etc.)
 */
enum BackupStrategyOutcome
{
    SUCCESS,
    INCORRECT_STRATEGY,
    CORRECT_STRATEGY_FAILED,
    ABSOLUTE_FAILURE
}
