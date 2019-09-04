/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.stresstests.transaction.checkpoint.mutation;

import org.neo4j.graphdb.Transaction;

interface Mutation
{
    void perform( Transaction tx, long nodeId, String value );
}
