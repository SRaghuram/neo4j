package com.neo4j.test;

import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
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
    private final BoltTransaction transaction;

    TestFabricTransaction( TransactionalContextFactory contextFactory, BoltTransaction transaction )
    {
        this.contextFactory = contextFactory;
        this.transaction = transaction;
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
        var result = new TestFabricResultSubscriber();
        try
        {
            BoltQueryExecution boltQueryExecution = transaction.executeQuery( query, params, false, result );
            result.materialize(boltQueryExecution.getQueryExecution());
        }
        catch ( QueryExecutionKernelException e )
        {
            throw new QueryExecutionException( "Query execution failed", e, e.status().code().serialize() );
        }

        return result;
    }

    @Override
    public void terminate()
    {
        transaction.markForTermination( Terminated );
    }

    @Override
    public void commit()
    {
        try
        {
            transaction.commit();
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
            transaction.rollback();
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
        throw fail();
    }

    @Override
    public Node createNode( Label... labels )
    {
        throw fail();
    }

    @Override
    public Node getNodeById( long id )
    {
        throw fail();
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        throw fail();
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        throw fail();
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        throw fail();
    }

    @Override
    public Iterable<Label> getAllLabelsInUse()
    {
        throw fail();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        throw fail();
    }

    @Override
    public Iterable<Label> getAllLabels()
    {
        throw fail();
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes()
    {
        throw fail();
    }

    @Override
    public Iterable<String> getAllPropertyKeys()
    {
        throw fail();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, String template, StringSearchMode searchMode )
    {
        throw fail();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, Map<String,Object> propertyValues )
    {
        throw fail();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3 )
    {
        throw fail();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2 )
    {
        throw fail();
    }

    @Override
    public Node findNode( Label label, String key, Object value )
    {
        throw fail();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, Object value )
    {
        throw fail();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label )
    {
        throw fail();
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        throw fail();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        throw fail();
    }

    @Override
    public Lock acquireWriteLock( Entity entity )
    {
        throw fail();
    }

    @Override
    public Lock acquireReadLock( Entity entity )
    {
        throw fail();
    }

    @Override
    public Schema schema()
    {
        throw fail();
    }

    @Override
    public void setTransaction( KernelTransaction transaction )
    {
        throw fail();
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        throw fail();
    }

    @Override
    public KernelTransaction.Type transactionType()
    {
        throw fail();
    }

    @Override
    public SecurityContext securityContext()
    {
        throw fail();
    }

    @Override
    public ClientConnectionInfo clientInfo()
    {
        throw fail();
    }

    @Override
    public KernelTransaction.Revertable overrideWith( SecurityContext context )
    {
        throw fail();
    }

    @Override
    public Optional<Status> terminationReason()
    {
        throw fail();
    }

    @Override
    public void setMetaData( Map<String,Object> txMeta )
    {
        throw fail();
    }

    @Override
    public void checkInTransaction()
    {
        throw fail();
    }

    @Override
    public boolean isOpen()
    {
        throw fail();
    }

    @Override
    public Relationship newRelationshipEntity( long id )
    {
        throw fail();
    }

    @Override
    public Relationship newRelationshipEntity( long id, long startNodeId, int typeId, long endNodeId )
    {
        throw fail();
    }

    @Override
    public Node newNodeEntity( long nodeId )
    {
        throw fail();
    }

    @Override
    public RelationshipType getRelationshipTypeById( int type )
    {
        throw fail();
    }

    private UnsupportedOperationException fail()
    {
        return new UnsupportedOperationException( "This method is not implemented in fabric test facade." );
    }
}
