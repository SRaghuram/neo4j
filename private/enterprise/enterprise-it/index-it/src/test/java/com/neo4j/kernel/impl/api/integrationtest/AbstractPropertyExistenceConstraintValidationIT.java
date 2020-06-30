/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class AbstractPropertyExistenceConstraintValidationIT extends KernelIntegrationTest
{
    abstract void createConstraint( String key, String property ) throws KernelException;

    abstract long createEntity( KernelTransaction transaction, String type ) throws Exception;

    abstract long createEntity( KernelTransaction transaction, String property, String value ) throws Exception;

    abstract long createEntity( KernelTransaction transaction, String type, String property, String value )
            throws Exception;

    abstract long createConstraintAndEntity( String type, String property, String value ) throws Exception;

    abstract void setProperty( Write writeOps, long entityId, int propertyKeyId, Value value )
            throws Exception;

    abstract void removeProperty( Write writeOps, long entityId, int propertyKey ) throws Exception;

    abstract int entityCount() throws TransactionFailureException;

    @Override
    protected DatabaseManagementService createDatabaseService()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDir.homePath() ).setFileSystem( testDir.getFileSystem() )
                                                                                       .build();
    }

    @Test
    void shouldEnforcePropertyExistenceConstraintWhenCreatingEntityWithoutProperty() throws Exception
    {
        // given
        createConstraint( "Type1", "key1" );

        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        createEntity( transaction, "Type1" );
        var e = assertThrows( ConstraintViolationException.class, this::commit );
        var rootCause = (Status.HasStatus) getRootCause( e );
        assertThat( rootCause.status() ).isEqualTo( Status.Schema.ConstraintValidationFailed );
    }

    @Test
    void shouldEnforceConstraintWhenRemoving() throws Exception
    {
        // given
        long entity = createConstraintAndEntity( "Type1", "key1", "value1" );
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
        removeProperty( transaction.dataWrite(), entity, key );

        var e = assertThrows( ConstraintViolationException.class, this::commit );
        var rootCause = (Status.HasStatus) getRootCause( e );
        assertThat( rootCause.status() ).isEqualTo( Status.Schema.ConstraintValidationFailed );
    }

    @Test
    void shouldAllowTemporaryViolationsWithinTransactions() throws Exception
    {
        // given
        long entity = createConstraintAndEntity( "Type1", "key1", "value1" );
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
        //remove and put back
        removeProperty( transaction.dataWrite(), entity, key );
        setProperty( transaction.dataWrite(), entity, key, Values.of( "value2" ) );

        commit();
    }

    @Test
    void shouldAllowNoopPropertyUpdate() throws Exception
    {
        // given
        long entity = createConstraintAndEntity( "Type1", "key1", "value1" );

        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
        setProperty( transaction.dataWrite(), entity, key, Values.of( "value1" ) );

        // then should not throw exception
    }

    @Test
    void shouldAllowCreationOfNonConflictingData() throws Exception
    {
        // given
        createConstraintAndEntity( "Type1", "key1", "value1" );

        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        createEntity( transaction, "key1", "value1" );
        createEntity( transaction, "Type2" );
        createEntity( transaction, "Type1", "key1", "value2" );
        createEntity( transaction, "Type1", "key1", "value3" );

        commit();

        // then
        assertEquals( 5, entityCount() );
    }
}
