/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.operators;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.temporal.TemporalUtil;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static java.lang.String.format;

public class Warmup
{
    public static void warmup( GraphDatabaseAPI db, LoggingService loggingService ) throws DbException
    {
        TemporalUtil temporal = new TemporalUtil();

        int dbPageSizeBytes = (int) ByteUnit.kibiBytes( 8 );
        int nodesPerPage = dbPageSizeBytes / NodeRecordFormat.RECORD_SIZE;
        int relationshipsPerPage = dbPageSizeBytes / RelationshipRecordFormat.RECORD_SIZE;

        NeoStores neoStore = getNeoStores( db );

        long nodeStoreWarmupStart = System.currentTimeMillis();

        warmNodeStore(
                db,
                neoStore.getNodeStore().getHighestPossibleIdInUse(),
                nodesPerPage );

        long relationshipStoreWarmupStart = System.currentTimeMillis();
        loggingService.info( format( "Node store warmed in: %s",
                temporal.milliDurationToString( relationshipStoreWarmupStart - nodeStoreWarmupStart ) ) );

        warmRelationshipStore(
                db,
                neoStore.getRelationshipStore().getHighestPossibleIdInUse(),
                relationshipsPerPage );

        loggingService.info( format( "Relationship store warmed in: %s",
                temporal.milliDurationToString( System.currentTimeMillis() - relationshipStoreWarmupStart ) ) );
    }

    private static void warmNodeStore(
            GraphDatabaseAPI db,
            long highestNodeKey,
            int nodesPerPage ) throws DbException
    {
        try ( Transaction ignore = db.beginTx() )
        {
            ReadOperations readOperations = getReadOperations( db );
            for ( int i = 0; i <= highestNodeKey; i = i + nodesPerPage )
            {
                Cursor<NodeItem> cursor = readOperations.nodeCursorById( i );
                cursor.next();
                cursor.close();
            }
        }
        catch ( Exception e )
        {
            throw new DbException( "Error during warmup of node stores", e );
        }
    }

    private static void warmRelationshipStore(
            GraphDatabaseAPI db,
            long highestRelationshipKey,
            int relationshipsPerPage ) throws DbException
    {
        try ( Transaction ignore = db.beginTx() )
        {
            ReadOperations readOperations = getReadOperations( db );
            for ( int i = 0; i <= highestRelationshipKey; i = i + relationshipsPerPage )
            {
                Cursor<RelationshipItem> cursor = readOperations.relationshipCursorById( i );
                cursor.next();
                cursor.close();
            }
        }
        catch ( Exception e )
        {
            throw new DbException( "Error during warmup of relationship store", e );
        }
    }

    private static NeoStores getNeoStores( GraphDatabaseAPI db )
    {
        return getStoreAccess( db ).getRawNeoStores();
    }

    private static StoreAccess getStoreAccess( GraphDatabaseAPI db )
    {
        return new StoreAccess( getRecordStoreEngine( db ).testAccessNeoStores() );
    }

    private static RecordStorageEngine getRecordStoreEngine( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
    }

    private static ReadOperations getReadOperations( GraphDatabaseAPI db )
    {
        return getContextBridge( db ).get().readOperations();
    }

    private static ThreadToStatementContextBridge getContextBridge( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }
}
