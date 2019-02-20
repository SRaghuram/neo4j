/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.temporal.TemporalUtil;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;

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
        Kernel kernel = getKernel( db );
        try ( Transaction tx = startTransaction( kernel ) )
        {
            CursorFactory cursors = tx.cursors();
            Read read = tx.dataRead();
            try ( NodeCursor nodeCursor = cursors.allocateNodeCursor() )
            {
                for ( int i = 0; i <= highestNodeKey; i = i + nodesPerPage )
                {
                    read.singleNode( i, nodeCursor );
                    nodeCursor.next();
                }
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
        Kernel kernel = getKernel( db );
        try ( Transaction tx = startTransaction( kernel ) )
        {
            CursorFactory cursors = tx.cursors();
            Read read = tx.dataRead();
            try ( RelationshipScanCursor relationshipCursor = cursors.allocateRelationshipScanCursor() )
            {
                for ( int i = 0; i <= highestRelationshipKey; i = i + relationshipsPerPage )
                {
                    read.singleRelationship( i, relationshipCursor );
                    relationshipCursor.next();
                }
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

    private static Transaction startTransaction( Kernel kernel )
            throws TransactionFailureException
    {
        return kernel.beginTransaction( implicit, SecurityContext.AUTH_DISABLED );
    }

    private static Kernel getKernel( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( Kernel.class );
    }
}
