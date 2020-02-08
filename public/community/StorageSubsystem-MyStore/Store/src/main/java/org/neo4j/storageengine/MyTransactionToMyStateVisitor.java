package org.neo4j.storageengine;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import java.util.Collection;
import java.util.Iterator;

public class MyTransactionToMyStateVisitor extends TxStateVisitor.Adapter{

    ReadableTransactionState txState;
    SchemaState schemaState;
    SchemaRuleAccess schemaStorage;
    ConstraintRuleAccessor constraintSemantics;
    Collection<StorageCommand> commands;
    MyTransactionToMyStateVisitor(Collection<StorageCommand> commands, ReadableTransactionState txState, SchemaState schemaState, SchemaRuleAccess schemaRuleAccess,
                                  ConstraintRuleAccessor constraintSemantics )
    {
        this.commands = commands;
        this.txState = txState;
        this.schemaState = schemaState;
        this.schemaStorage = schemaRuleAccess;
        this.constraintSemantics = constraintSemantics;
    }
    @Override
    public void visitCreatedNode( long id )
    {
        SystemOutPrintln("id:["+id+"]");
    }

    @Override
    public void visitDeletedNode( long id )
    {
        SystemOutPrintln("id:["+id+"]");
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
    {
        SystemOutPrintln("id:["+id+"] type["+type+"] ["+startNode+":"+endNode+"]");
    }

    @Override
    public void visitDeletedRelationship( long id )
    {
        SystemOutPrintln("id:["+id+"]");
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added,
                                          Iterator<StorageProperty> changed, IntIterable removed )
    {
        StringBuilder strb = new StringBuilder();
        strb.append("NodeProps["+id+"]-");
        while (added.hasNext())
        {
            StorageProperty str = added.next();
            strb.append(" added["+str.propertyKeyId()+":"+ str.value()+"]");
        }
        while (changed.hasNext())
        {
            StorageProperty str = changed.next();
            strb.append(" changed["+str.propertyKeyId()+":"+str.value()+"]");
        }
        IntIterator intr = removed.intIterator();
        while (intr.hasNext())
        {
            strb.append(" removed["+intr.next()+"]");
        }
        SystemOutPrintln(strb.toString());
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added,
                                         Iterator<StorageProperty> changed, IntIterable removed )
    {
        StringBuilder strb = new StringBuilder();
        strb.append("RelProps["+id+"]-");
        while (added.hasNext())
        {
            StorageProperty str = added.next();
            strb.append(" added["+str.propertyKeyId()+":"+str.value()+"]");
        }
        while (changed.hasNext())
        {
            StorageProperty str = changed.next();
            strb.append(" changed["+str.propertyKeyId()+":"+str.value()+"]");
        }
        IntIterator intr = removed.intIterator();
        while (intr.hasNext())
        {
            strb.append(" removed["+intr.next()+"]");
        }
        SystemOutPrintln(strb.toString());
    }

    @Override
    public void visitNodeLabelChanges(long id, LongSet added, LongSet removed )
    {
        StringBuilder strb = new StringBuilder();
        strb.append("Label["+id+"]-");
        LongIterator intr = added.   longIterator();
        while (intr.hasNext())
        {
            strb.append(" added["+intr.next()+"]");
        }
        intr = removed.longIterator();
        while (intr.hasNext())
        {
            strb.append(" removed["+intr.next()+"]");
        }
        SystemOutPrintln(strb.toString());
    }

    @Override
    public void visitAddedIndex( IndexDescriptor index ) throws KernelException
    {
        SystemOutPrintln("");
    }

    @Override
    public void visitRemovedIndex( IndexDescriptor index )
    {
        SystemOutPrintln("");
    }

    @Override
    public void visitAddedConstraint( ConstraintDescriptor element ) throws KernelException
    {
        SystemOutPrintln("");
    }

    @Override
    public void visitRemovedConstraint( ConstraintDescriptor element )
    {
        SystemOutPrintln("");
    }

    @Override
    public void visitCreatedLabelToken( long id, String name, boolean internal )
    {
        SystemOutPrintln("");
    }

    @Override
    public void visitCreatedPropertyKeyToken( long id, String name, boolean internal )
    {
        SystemOutPrintln("");
    }

    @Override
    public void visitCreatedRelationshipTypeToken( long id, String name, boolean internal )
    {
        SystemOutPrintln("");
    }

    @Override
    public void close()
    {
        SystemOutPrintln("");
    }
    
    private void SystemOutPrintln(String msg)
    {
        System.out.println("MyTransactionToMyStateVisitor::" + Thread.currentThread().getStackTrace()[2].getMethodName()+":"+msg);
    }
}
