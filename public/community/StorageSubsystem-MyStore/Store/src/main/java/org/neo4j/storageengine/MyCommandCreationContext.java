package org.neo4j.storageengine;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.CommandCreationContext;

public class MyCommandCreationContext implements CommandCreationContext {

        MyStore myStore;
        MyCommandCreationContext( MyStore store )
        {
            this.myStore = store;
        }
        @Override
        public long reserveNode() {
            return myStore.getNextId(MyStore.MyStoreType.NODE);
        }

        @Override
        public long reserveRelationship() {
            return myStore.getNextId(MyStore.MyStoreType.RELATIONSHIP);
        }

        @Override
        public long reserveSchema() {
            return 0;
        }


        @Override
        public int reserveLabelTokenId() {
            return myStore.getNextKeyId(MyStore.KEYTYPE.LABEL);
        }

        @Override
        public int reservePropertyKeyTokenId() {
            return myStore.getNextKeyId(MyStore.KEYTYPE.PROPERTY);
        }

        @Override
        public int reserveRelationshipTypeTokenId() {
            return myStore.getNextKeyId(MyStore.KEYTYPE.RELTYPE);
        }

        @Override
        public void close() {

        }

        MyTransactionState createTransactionRecordState( long lastTransactionIdWhenStarted,
                                                             ResourceLocker locks )
        {
                //RecordChangeSet recordChangeSet = new RecordChangeSet( loaders );
                return new MyTransactionState( myStore);// integrityValidator,
                        //recordChangeSet, lastTransactionIdWhenStarted, locks,
                        //relationshipCreator, relationshipDeleter, propertyCreator, propertyDeleter );
        }
}
