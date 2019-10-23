/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.transaction;

import com.neo4j.fabric.executor.FabricLocalExecutor;
import com.neo4j.fabric.executor.FabricRemoteExecutor;
import com.neo4j.fabric.stream.StatementResult;

import java.util.Optional;
import java.util.function.Function;

import org.neo4j.kernel.api.exceptions.Status;

public interface FabricTransaction
{

    void begin();

    void commit();

    void rollback();

    StatementResult execute( Function<FabricExecutionContext,StatementResult> runLogic );

    void markForTermination( Status reason );

    Optional<Status> getReasonIfTerminated();

    FabricTransactionInfo getTransactionInfo();

    interface FabricExecutionContext
    {
        FabricRemoteExecutor.FabricRemoteTransaction getRemote();

        FabricLocalExecutor.FabricLocalTransaction getLocal();
    }
}
