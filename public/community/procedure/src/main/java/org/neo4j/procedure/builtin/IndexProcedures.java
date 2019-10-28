/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.procedure.builtin;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.helpers.Indexes;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailure;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;

public class IndexProcedures
{
    private final KernelTransaction ktx;
    private final IndexingService indexingService;

    public IndexProcedures( KernelTransaction tx, IndexingService indexingService )
    {
        this.ktx = tx;
        this.indexingService = indexingService;
    }

    void awaitIndexByName( String indexName, long timeout, TimeUnit timeoutUnits )
            throws ProcedureException
    {
        final IndexDescriptor index = getIndex( indexName );
        waitUntilOnline( index, timeout, timeoutUnits );
    }

    void resampleIndex( String indexName ) throws ProcedureException
    {
        final IndexDescriptor index = getIndex( indexName );
        triggerSampling( index );
    }

    void resampleOutdatedIndexes()
    {
        indexingService.triggerIndexSampling( IndexSamplingMode.TRIGGER_REBUILD_UPDATED );
    }

    void awaitIndexResampling( long timeout ) throws ProcedureException
    {
        try
        {
            Indexes.awaitResampling( ktx.schemaRead(), timeout);
        }
        catch ( TimeoutException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureTimedOut, e, "Index resampling timed out" );
        }
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createIndex( String indexName, List<String> labels, List<String> properties,
            String providerName ) throws ProcedureException
    {
        return createIndex( indexName, labels, properties, providerName,
                "index created", ( schemaWrite, name, descriptor, provider ) -> schemaWrite.indexCreate( descriptor, provider, name ) );
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createUniquePropertyConstraint( String constraintName, List<String> labels, List<String> properties,
            String providerName )
            throws ProcedureException
    {
        return createIndex( constraintName, labels, properties, providerName,
                "uniqueness constraint online",
                ( schemaWrite, name, schema, provider ) -> schemaWrite.uniquePropertyConstraintCreate(
                        IndexPrototype.uniqueForSchema( schema, schemaWrite.indexProviderByName( provider ) ).withName( name ) ) );
    }

    public Stream<BuiltInProcedures.SchemaIndexInfo> createNodeKey( String constraintName, List<String> labels, List<String> properties, String providerName )
            throws ProcedureException
    {
        return createIndex( constraintName, labels, properties, providerName,
                "node key constraint online",
                ( schemaWrite, name, schema, provider ) -> schemaWrite.nodeKeyConstraintCreate(
                        IndexPrototype.uniqueForSchema( schema, schemaWrite.indexProviderByName( provider ) ).withName( name ) ) );
    }

    private Stream<BuiltInProcedures.SchemaIndexInfo> createIndex( String name, List<String> labels, List<String> properties, String providerName,
            String statusMessage, IndexCreator indexCreator ) throws ProcedureException
    {
        assertProviderNameNotNull( providerName );
        assertSingleLabel( labels );
        int labelId = getOrCreateLabelId( labels.get( 0 ) );
        int[] propertyKeyIds = getOrCreatePropertyIds( properties );
        try
        {
            SchemaWrite schemaWrite = ktx.schemaWrite();
            LabelSchemaDescriptor labelSchemaDescriptor = SchemaDescriptor.forLabel( labelId, propertyKeyIds );
            indexCreator.create( schemaWrite, name, labelSchemaDescriptor, providerName );
            return Stream.of( new BuiltInProcedures.SchemaIndexInfo( name, labels, properties, providerName, statusMessage ) );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( e.status(), e, e.getMessage() );
        }
    }

    private static void assertSingleLabel( List<String> labels ) throws ProcedureException
    {
        if ( labels.size() != 1 )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "Could not create index with specified label(s), need to provide exactly one but was " + labels );
        }
    }

    private static void assertProviderNameNotNull( String providerName ) throws ProcedureException
    {
        if ( providerName == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, indexProviderNullMessage() );
        }
    }

    private static String indexProviderNullMessage()
    {
        return "Could not create index with specified index provider being null.";
    }

    private int getLabelId( String labelName ) throws ProcedureException
    {
        int labelId = ktx.tokenRead().nodeLabel( labelName );
        if ( labelId == TokenRead.NO_TOKEN )
        {
            throw new ProcedureException( Status.Schema.LabelAccessFailed, "No such label %s", labelName );
        }
        return labelId;
    }

    private int[] getPropertyIds( String[] propertyKeyNames ) throws ProcedureException
    {
        int[] propertyKeyIds = new int[propertyKeyNames.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {

            int propertyKeyId = ktx.tokenRead().propertyKey( propertyKeyNames[i] );
            if ( propertyKeyId == TokenRead.NO_TOKEN )
            {
                throw new ProcedureException( Status.Schema.PropertyKeyAccessFailed, "No such property key %s", propertyKeyNames[i] );
            }
            propertyKeyIds[i] = propertyKeyId;
        }
        return propertyKeyIds;
    }

    private int getOrCreateLabelId( String labelName ) throws ProcedureException
    {
        try
        {
            return ktx.tokenWrite().labelGetOrCreateForName( labelName );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( e.status(), e, e.getMessage() );
        }
    }

    private int[] getOrCreatePropertyIds( List<String> propertyKeyNames ) throws ProcedureException
    {
        int[] propertyKeyIds = new int[propertyKeyNames.size()];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            try
            {
                propertyKeyIds[i] = ktx.tokenWrite().propertyKeyGetOrCreateForName( propertyKeyNames.get( i ) );
            }
            catch ( KernelException e )
            {
                throw new ProcedureException( e.status(), e, e.getMessage() );
            }
        }
        return propertyKeyIds;
    }

    private IndexDescriptor getIndex( String indexName ) throws ProcedureException
    {
        // Find index by name.
        IndexDescriptor indexReference = ktx.schemaRead().indexGetForName( indexName );

        if ( indexReference == IndexDescriptor.NO_INDEX )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, "No such index '%s'", indexName );
        }
        return indexReference;
    }

    private void waitUntilOnline( IndexDescriptor index, long timeout, TimeUnit timeoutUnits ) throws ProcedureException
    {
        try
        {
            Predicates.awaitEx( () -> isOnline( index ), timeout, timeoutUnits );
        }
        catch ( TimeoutException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureTimedOut, "Index on '%s' did not come online within %s %s",
                    index.userDescription( new SilentTokenNameLookup( ktx.tokenRead() ) ), timeout, timeoutUnits );
        }
    }

    private boolean isOnline( IndexDescriptor index ) throws ProcedureException
    {
        InternalIndexState state = getState( index );
        switch ( state )
        {
            case POPULATING:
                return false;
            case ONLINE:
                return true;
            case FAILED:
                String cause = getFailure( index );
                throw new ProcedureException( Status.Schema.IndexCreationFailed,
                        IndexPopulationFailure.appendCauseOfFailure( "Index '%s' is in failed state.", cause ), index.getName() );
            default:
                throw new IllegalStateException( "Unknown index state " + state );
        }
    }

    private InternalIndexState getState( IndexDescriptor index ) throws ProcedureException
    {
        try
        {
            return ktx.schemaRead().indexGetState( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e, "No such index %s", index.getName() );
        }
    }

    private String getFailure( IndexDescriptor index ) throws ProcedureException
    {
        try
        {
            return ktx.schemaRead().indexGetFailure( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, e, "No such index %s", index.getName() );
        }
    }

    private void triggerSampling( IndexDescriptor index )
    {
        indexingService.triggerIndexSampling( index, IndexSamplingMode.TRIGGER_REBUILD_ALL );
    }

    @FunctionalInterface
    private interface IndexCreator
    {
        void create( SchemaWrite schemaWrite, String name, LabelSchemaDescriptor descriptor, String providerName ) throws KernelException;
    }
}
