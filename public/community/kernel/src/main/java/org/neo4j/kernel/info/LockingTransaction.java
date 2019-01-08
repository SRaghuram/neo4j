/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.info;

import java.beans.ConstructorProperties;
import java.io.Serializable;

public class LockingTransaction implements Serializable
{
    private static final long serialVersionUID = -8743172898557855333L;

    private String transaction;
    private int readCount;
    private int writeCount;

    @ConstructorProperties( { "transaction", "readCount", "writeCount" } )
    public LockingTransaction( String transaction, int readCount, int writeCount )
    {
        this.transaction = transaction;
        this.readCount = readCount;
        this.writeCount = writeCount;
    }

    @Override
    public String toString()
    {
        return transaction+"{" +"readCount=" + readCount + ", writeCount=" + writeCount + "}";
    }

    public String getTransaction()
    {
        return transaction;
    }

    public int getReadCount()
    {
        return readCount;
    }

    public int getWriteCount()
    {
        return writeCount;
    }
}
