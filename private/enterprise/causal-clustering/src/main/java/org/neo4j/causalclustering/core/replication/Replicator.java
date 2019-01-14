/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import java.util.concurrent.Future;

/**
 * Replicate content across a cluster of servers.
  */
public interface Replicator
{
    /**
     * Submit content for replication. This method does not guarantee that the content
     * actually gets replicated, it merely makes an attempt at replication. Other
     * mechanisms must be used to achieve required delivery semantics.
     *
     * @param content      The content to replicated.
     * @param trackResult  Whether to track the result for this operation.
     *
     * @return A future that will receive the result when available. Only valid if trackResult is set.
     */
    Future<Object> replicate( ReplicatedContent content, boolean trackResult ) throws ReplicationFailureException;
}
