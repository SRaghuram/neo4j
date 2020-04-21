/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.fabric;

import com.neo4j.fabric.executor.FabricException;

import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.cypher.internal.javacompat.ResultSubscriber;
import org.neo4j.exceptions.Neo4jException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.util.ValueUtils;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

public class TestFabricTransaction implements InternalTransaction
{

    private final TransactionalContextFactory contextFactory;
    private final BoltTransaction fabricTransaction;
    private final InternalTransaction kernelInternalTransaction;

    TestFabricTransaction( TransactionalContextFactory contextFactory, BoltTransaction transaction, InternalTransaction kernelInternalTransaction )
    {
        this.contextFactory = contextFactory;
        this.fabricTransaction = transaction;
        this.kernelInternalTransaction = kernelInternalTransaction;
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return execute( query, Map.of() );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        var params = ValueUtils.asParameterMapValue( parameters );
        var result = new ResultSubscriber( null, new TestFabricValueMapper() );
        try
        {
            BoltQueryExecution boltQueryExecution = fabricTransaction.executeQuery( query, params, false, result );
            result.materialize( boltQueryExecution.getQueryExecution() );
        }
        catch ( QueryExecutionKernelException | Neo4jException | FabricException e )
        {
            throw new QueryExecutionException( e.getMessage(), e, e.status().code().serialize() );
        }

        return result;
    }

    @Override
    public void terminate()
    {
        fabricTransaction.markForTermination( Terminated );
    }

    @Override
    public void commit()
    {
        try
        {
            fabricTransaction.commit();
        }
        catch ( TransactionFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Unable to complete transaction.", e );
        }
    }

    @Override
    public void rollback()
    {
        try
        {
            fabricTransaction.rollback();
        }
        catch ( TransactionFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Unable to complete transaction.", e );
        }
    }

    @Override
    public void close()
    {
    }

    @Override
    public Node createNode()
    {
        return kernelInternalTransaction.createNode();
    }

    @Override
    public Node createNode( Label... labels )
    {
        return kernelInternalTransaction.createNode( labels );
    }

    @Override
    public Node getNodeById( long id )
    {
        return kernelInternalTransaction.getNodeById( id );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return kernelInternalTransaction.getRelationshipById( id );
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return kernelInternalTransaction.bidirectionalTraversalDescription();
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return kernelInternalTransaction.traversalDescription();
    }

    @Override
    public Iterable<Label> getAllLabelsInUse()
    {
        return kernelInternalTransaction.getAllLabelsInUse();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        return kernelInternalTransaction.getAllRelationshipTypesInUse();
    }

    @Override
    public Iterable<Label> getAllLabels()
    {
        return kernelInternalTransaction.getAllLabels();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes()
    {
        return kernelInternalTransaction.getAllRelationshipTypes();
    }

    @Override
    public Iterable<String> getAllPropertyKeys()
    {
        return kernelInternalTransaction.getAllPropertyKeys();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, String template, StringSearchMode searchMode )
    {
        return kernelInternalTransaction.findNodes( label, key, template, searchMode );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, Map<String,Object> propertyValues )
    {
        return kernelInternalTransaction.findNodes( label, propertyValues );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3 )
    {
        return kernelInternalTransaction.findNodes( label, key1, value1, key2, value2, key3, value3 );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2 )
    {
        return kernelInternalTransaction.findNodes( label, key1, value1, key2, value2 );
    }

    @Override
    public Node findNode( Label label, String key, Object value )
    {
        return kernelInternalTransaction.findNode( label, key, value );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, Object value )
    {
        return kernelInternalTransaction.findNodes( label, key, value );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label )
    {
        return kernelInternalTransaction.findNodes( label );
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        return kernelInternalTransaction.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        return kernelInternalTransaction.getAllRelationships();
    }

    @Override
    public Lock acquireWriteLock( Entity entity )
    {
        return kernelInternalTransaction.acquireWriteLock( entity );
    }

    @Override
    public Lock acquireReadLock( Entity entity )
    {
        return kernelInternalTransaction.acquireReadLock( entity );
    }

    @Override
    public Schema schema()
    {
        return kernelInternalTransaction.schema();
    }

    @Override
    public void setTransaction( KernelTransaction transaction )
    {
        kernelInternalTransaction.setTransaction( transaction );
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        return kernelInternalTransaction.kernelTransaction();
    }

    @Override
    public KernelTransaction.Type transactionType()
    {
        return kernelInternalTransaction.transactionType();
    }

    @Override
    public SecurityContext securityContext()
    {
        return kernelInternalTransaction.securityContext();
    }

    @Override
    public ClientConnectionInfo clientInfo()
    {
        return kernelInternalTransaction.clientInfo();
    }

    @Override
    public KernelTransaction.Revertable overrideWith( SecurityContext context )
    {
        return kernelInternalTransaction.overrideWith( context );
    }

    @Override
    public Optional<Status> terminationReason()
    {
        return kernelInternalTransaction.terminationReason();
    }

    @Override
    public void setMetaData( Map<String,Object> txMeta )
    {
        kernelInternalTransaction.setMetaData( txMeta );
    }

    @Override
    public void checkInTransaction()
    {
        kernelInternalTransaction.checkInTransaction();
    }

    @Override
    public boolean isOpen()
    {
        return kernelInternalTransaction.isOpen();
    }

    @Override
    public Relationship newRelationshipEntity( long id )
    {
        return kernelInternalTransaction.newRelationshipEntity( id );
    }

    @Override
    public Relationship newRelationshipEntity( long id, long startNodeId, int typeId, long endNodeId )
    {
        return kernelInternalTransaction.newRelationshipEntity( id, startNodeId, typeId, endNodeId );
    }

    @Override
    public Node newNodeEntity( long nodeId )
    {
        return kernelInternalTransaction.newNodeEntity( nodeId );
    }

    @Override
    public RelationshipType getRelationshipTypeById( int type )
    {
        return kernelInternalTransaction.getRelationshipTypeById( type );
    }
}