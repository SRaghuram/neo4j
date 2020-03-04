/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.executor.Location;
import com.neo4j.fabric.executor.SingleDbTransaction;

import java.util.function.Supplier;

/**
 * A container for {@link SingleDbTransaction}s.
 */
public interface CompositeTransaction
{
    /**
     * Starts and registers a transaction that is known to do writes.
     */
    <TX extends SingleDbTransaction> TX startWritingTransaction( Location location, Supplier<TX> writingTransactionSupplier ) throws FabricException;

    /**
     * Starts and registers a transaction that is so far known to do only reads. Such transaction can be later upgraded to a writing
     * one using {@link #upgradeToWritingTransaction(SingleDbTransaction)}
     */
    <TX extends SingleDbTransaction> TX startReadingTransaction( Location location, Supplier<TX> readingTransactionSupplier ) throws FabricException;

    /**
     * Starts and registers a transaction that will do only reads. Such transaction cannot be later upgraded to a writing
     * one using {@link #upgradeToWritingTransaction(SingleDbTransaction)}
     */
    <TX extends SingleDbTransaction> TX startReadingOnlyTransaction( Location location, Supplier<TX> readingTransactionSupplier ) throws FabricException;

    <TX extends SingleDbTransaction> void upgradeToWritingTransaction( TX writingTransaction ) throws FabricException;

}
