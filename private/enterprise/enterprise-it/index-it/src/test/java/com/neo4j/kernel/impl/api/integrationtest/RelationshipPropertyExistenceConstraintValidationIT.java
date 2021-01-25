/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import java.util.UUID;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;

class RelationshipPropertyExistenceConstraintValidationIT extends AbstractPropertyExistenceConstraintValidationIT
{
    @Override
    void createConstraint( String key, String property ) throws KernelException
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        int relTypeId = tokenWrite.relationshipTypeGetOrCreateForName( key );
        int propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( property );
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        schemaWrite.relationshipPropertyExistenceConstraintCreate( forRelType( relTypeId, propertyKeyId ), null );
        commit();
    }

    @Override
    long createEntity( KernelTransaction transaction, String type ) throws Exception
    {
        long start = transaction.dataWrite().nodeCreate();
        long end = transaction.dataWrite().nodeCreate();
        int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( type );
        return transaction.dataWrite().relationshipCreate(  start, relType, end );
    }

    @Override
    long createEntity( KernelTransaction transaction, String property, String value ) throws Exception
    {
        long start = transaction.dataWrite().nodeCreate();
        long end = transaction.dataWrite().nodeCreate();
        String relationshipTypeName = UUID.randomUUID().toString();
        int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( relationshipTypeName );
        long relationship = transaction.dataWrite().relationshipCreate( start, relType, end );

        int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
        transaction.dataWrite().relationshipSetProperty( relationship, propertyKey, Values.of( value ) );
        return relationship;
    }

    @Override
    long createEntity( KernelTransaction transaction, String type, String property, String value ) throws Exception
    {
        long relationship = createEntity( transaction, type );
        int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
        transaction.dataWrite().relationshipSetProperty( relationship, propertyKey, Values.of( value ) );
        return relationship;
    }

    @Override
    long createConstraintAndEntity( String type, String property, String value ) throws Exception
    {
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( type );
        long start = transaction.dataWrite().nodeCreate();
        long end = transaction.dataWrite().nodeCreate();
        long relationship = transaction.dataWrite().relationshipCreate( start, relType, end );
        int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
        transaction.dataWrite().relationshipSetProperty( relationship, propertyKey, Values.of( value ) );
        commit();

        createConstraint( type, property );

        return relationship;
    }

    @Override
    void setProperty( Write writeOps, long entityId, int propertyKeyId, Value value ) throws Exception
    {
        writeOps.relationshipSetProperty( entityId, propertyKeyId, value );
    }

    @Override
    void removeProperty( Write writeOps, long entityId, int propertyKey ) throws Exception
    {
        writeOps.relationshipRemoveProperty( entityId, propertyKey );
    }

    @Override
    int entityCount() throws TransactionFailureException
    {
        KernelTransaction transaction = newTransaction();
        int result = KernelIntegrationTest.countRelationships( transaction );
        rollback();
        return result;
    }
}
