/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.kernel;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.benchmarks.KernelTxBatch;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.RelationshipDefinition;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class AbstractKernelBenchmark extends BaseDatabaseBenchmark
{
    private Kernel kernel;

    protected abstract KernelImplementation kernelImplementation();

    @Override
    public String benchmarkGroup()
    {
        return "Kernel API";
    }

    @Override
    public void afterDatabaseStart( DataGeneratorConfig config )
    {
        kernel = kernelImplementation().start( (GraphDatabaseAPI) this.db() );
    }

    static class TxState
    {
        private static final int DEFAULT_TX_BATCH_SIZE = 1;

        protected KernelTxBatch kernelTx;

        protected void initializeTx( AbstractKernelBenchmark benchmark ) throws KernelException
        {
            initializeTx( benchmark, DEFAULT_TX_BATCH_SIZE );
        }

        protected void initializeTx( AbstractKernelBenchmark benchmark, int txBatchSize ) throws KernelException
        {
            kernelTx = new KernelTxBatch( benchmark.kernel, txBatchSize );
        }

        void closeTx() throws Exception
        {
            kernelTx.closeTx();
        }

        int labelToId( Label l )
        {
            return kernelTx.token.nodeLabel( l.name() );
        }

        int[] labelsToIds( Label[] ls )
        {
            int[] tokens = new int[ls.length];
            for ( int i = 0; i < tokens.length; i++ )
            {
                tokens[i] = labelToId( ls[i] );
            }
            return tokens;
        }

        int[] propertyKeysToIds( String[] keys ) throws KernelException
        {
            int[] tokens = new int[keys.length];
            for ( int i = 0; i < tokens.length; i++ )
            {
                tokens[i] = kernelTx.token.propertyKeyGetOrCreateForName( keys[i] );
            }
            return tokens;
        }

        int relationshipTypeToId( RelationshipDefinition r )
        {
            return kernelTx.token.relationshipType( r.type().name() );
        }

        int propertyKeyToId( PropertyDefinition p )
        {
            return kernelTx.token.propertyKey( p.key() );
        }
    }
}
