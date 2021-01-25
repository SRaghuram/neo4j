/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

class NodePropertyExistenceConstraintValidationIT extends AbstractPropertyExistenceConstraintValidationIT
{
    @Test
    void shouldAllowNoopLabelUpdate() throws Exception
    {
        // given
        long entityId = createConstraintAndEntity( "Label1", "key1", "value1" );

        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int label = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
        transaction.dataWrite().nodeAddLabel( entityId, label );

        // then should not throw exception
    }

    @Override
    void createConstraint( String key, String property ) throws KernelException
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int label = tokenWrite.labelGetOrCreateForName( key );
        int propertyKey = tokenWrite.propertyKeyGetOrCreateForName( property );
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        schemaWrite.nodePropertyExistenceConstraintCreate( forLabel( label, propertyKey ), null );
        commit();
    }

    @Override
    long createEntity( KernelTransaction transaction, String type ) throws Exception
    {
        long node = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( type );
        transaction.dataWrite().nodeAddLabel( node, labelId );
        return node;
    }

    @Override
    long createEntity( KernelTransaction transaction, String property, String value ) throws Exception
    {
        long node = transaction.dataWrite().nodeCreate();
        int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
        transaction.dataWrite().nodeSetProperty( node, propertyKey, Values.of( value ) );
        return node;
    }

    @Override
    long createEntity( KernelTransaction transaction, String type, String property, String value ) throws Exception
    {
        long node = createEntity( transaction, type );
        int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
        transaction.dataWrite().nodeSetProperty( node, propertyKey, Values.of( value ) );
        return node;
    }

    @Override
    long createConstraintAndEntity( String type, String property, String value ) throws Exception
    {
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int label = transaction.tokenWrite().labelGetOrCreateForName( type );
        long node = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeAddLabel( node, label );
        int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
        transaction.dataWrite().nodeSetProperty( node, propertyKey, Values.of( value ) );
        commit();

        createConstraint( type, property );

        return node;
    }

    @Override
    void setProperty( Write writeOps, long entityId, int propertyKeyId, Value value ) throws Exception
    {
        writeOps.nodeSetProperty( entityId, propertyKeyId, value );
    }

    @Override
    void removeProperty( Write writeOps, long entityId, int propertyKey ) throws Exception
    {
        writeOps.nodeRemoveProperty( entityId, propertyKey );
    }

    @Override
    int entityCount() throws TransactionFailureException
    {
       KernelTransaction transaction = newTransaction();
        int result = countNodes( transaction );
        rollback();
        return result;
    }
}
