/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateHolder;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateProvider;

public class NullAuxiliaryTransactionStateManager implements org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateManager
{
    @Override
    public void registerProvider( AuxiliaryTransactionStateProvider provider )
    {
    }

    @Override
    public void unregisterProvider( AuxiliaryTransactionStateProvider provider )
    {
    }

    @Override
    public AuxiliaryTransactionStateHolder openStateHolder()
    {
        return null;
    }
}
