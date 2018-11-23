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
package org.neo4j.bolt.v3.runtime;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.v1.runtime.TransactionStateMachineV1SPI;
import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.values.virtual.MapValue;

public class TransactionStateMachineV3SPI extends TransactionStateMachineV1SPI
{
    public TransactionStateMachineV3SPI( DatabaseContext databaseContext, BoltChannel boltChannel, Duration txAwaitDuration, Clock clock )
    {
        super( databaseContext, boltChannel, txAwaitDuration, clock );
    }

    @Override
    protected BoltResultHandle newBoltResultHandle( String statement, MapValue params, TransactionalContext transactionalContext )
    {
        return new BoltResultHandleV3( statement, params, transactionalContext );
    }

    private class BoltResultHandleV3 extends BoltResultHandleV1
    {
        BoltResultHandleV3( String statement, MapValue params, TransactionalContext transactionalContext )
        {
            super( statement, params, transactionalContext );
        }

        @Override
        protected BoltResult newBoltResult( QueryResultProvider result, Clock clock )
        {
            return new CypherAdapterStreamV3( result.queryResult(), clock );
        }
    }
}
