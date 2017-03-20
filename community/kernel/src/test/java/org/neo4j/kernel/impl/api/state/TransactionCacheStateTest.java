/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

public class TransactionCacheStateTest extends TxStateTest
{
    public PageCacheRule pageCacheRule = new PageCacheRule();
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule );

    @Override
    protected TransactionState createTransactionState()
    {
            return null;//new PagedCache( pageCacheRule.getPageCache( fileSystemRule.get() ) );       
    }

    @After
    public void closeState() throws IOException
    {
        Closeable closeable = (Closeable) state;
        closeable.close();
    }
}
