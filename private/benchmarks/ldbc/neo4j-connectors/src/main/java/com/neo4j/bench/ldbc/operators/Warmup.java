/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.temporal.TemporalUtil;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

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
                neoStore.getNodeStore().getHighestPossibleIdInUse( NULL ),
                nodesPerPage );

        long relationshipStoreWarmupStart = System.currentTimeMillis();
        loggingService.info( format( "Node store warmed in: %s",
                temporal.milliDurationToString( relationshipStoreWarmupStart - nodeStoreWarmupStart ) ) );

        warmRelationshipStore(
                db,
                neoStore.getRelationshipStore().getHighestPossibleIdInUse( NULL ),
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
        try ( KernelTransaction tx = startTransaction( kernel ) )
        {
            CursorFactory cursors = tx.cursors();
            Read read = tx.dataRead();
            try ( NodeCursor nodeCursor = cursors.allocateNodeCursor( tx.pageCursorTracer() ) )
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
        try ( KernelTransaction tx = startTransaction( kernel ) )
        {
            CursorFactory cursors = tx.cursors();
            Read read = tx.dataRead();
            try ( RelationshipScanCursor relationshipCursor = cursors.allocateRelationshipScanCursor( tx.pageCursorTracer() ) )
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
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
    }

    private static KernelTransaction startTransaction( Kernel kernel )
            throws TransactionFailureException
    {
        return kernel.beginTransaction( IMPLICIT, SecurityContext.AUTH_DISABLED );
    }

    private static Kernel getKernel( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( Kernel.class );
    }
}
