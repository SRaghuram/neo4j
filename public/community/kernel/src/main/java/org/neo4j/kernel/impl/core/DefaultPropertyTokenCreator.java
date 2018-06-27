/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.locking.ResourceTypes;

public class DefaultPropertyTokenCreator extends IsolatedTransactionTokenCreator
{
    public DefaultPropertyTokenCreator( Supplier<Kernel> kernelSupplier )
    {
        super( kernelSupplier );
    }

    @Override
    protected int createKey( Transaction transaction, String name ) throws IllegalTokenNameException
    {
        transaction.locks().acquireSharedTokenCreateLock( ResourceTypes.TOKEN_CREATE_PROP_KEY );
        return transaction.tokenWrite().propertyKeyCreateForName( name );
    }
}
