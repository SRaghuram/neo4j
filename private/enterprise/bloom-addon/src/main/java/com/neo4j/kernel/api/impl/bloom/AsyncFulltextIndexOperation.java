/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import java.util.concurrent.ExecutionException;

/**
 * A fulltext index operation that is possibly in-flight, and whose completion can be waited for.
 */
public interface AsyncFulltextIndexOperation
{
    /**
     * Wait for the index operation to complete. Returns immediately if the operation has already completed.
     * @throws ExecutionException if the asynchronous operation failed with an exception.
     */
    void awaitCompletion() throws ExecutionException;
}
