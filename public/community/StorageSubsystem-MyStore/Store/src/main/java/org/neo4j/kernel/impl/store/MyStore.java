package org.neo4j.kernel.impl.store;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.batchimport.cache.ChunkedNumberArrayFactory;
import org.neo4j.internal.batchimport.cache.DynamicIntArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.DateValue;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.storageengine.api.StorageEntityScanCursor.NO_ID;

public class MyStore implements MetaDataStoreCommon{

    MyMetadataStore myMetadataStore = null;
    DatabaseLayout layout;
    public static final int START_ID=4;
    public static enum KEYTYPE
    {
        RELTYPE,
        PROPERTY,
        LABEL
    }
    public enum MyStoreType
    {
        NODE(4, "Nodes.mystore"),
        RELATIONSHIP(6, "Rels.mystore"),
        PROPERTY(3, "Props.mystore"),
        LABEL(2, "Labels.mystore"),
        LINK(4, "Links.mystore"),
        INCOMING(2, "Incoming.mystore"),
        OUTGOING(2, "Outgoing.mystore");

        int cellSize;
        String storeFileName;
        long currentId = START_ID;
        MyStoreType(int size, String fileName)
        {
            cellSize = size;
            this.storeFileName = fileName;//new File(storeDir.getAbsolutePath() + File.separator+ fileName);
        }

        int getCellSize()
        {
            return cellSize;
        }

        String getStoreFileName()
        {
            return storeFileName;
        }
        synchronized long getNextId()
        {
            return currentId++;
        }
        synchronized long getMaxId()
        {
            return currentId;
        }
        void resetId()
        { currentId = START_ID;}
        void setCurrentId(long curId)
        {
            currentId = curId;
        }
    }
    ChunkedNumberArrayFactory arrayFactory;
    HashMap<Integer, String> StringStore;
    HashMap<String, Integer> KeyMap, NodeMap;
    HashMap<Integer, Integer> KeyMapCount;
    DynamicIntArray LinkArray, Nodes, Rels, Props, Labels, outGoing, inComing;
    public static int NODE_SIZE= 4, REL_SIZE = 6, PROP_SIZE= 3, LABEL_SIZE=2, LINK_SIZE = 4;


    int IntId =1, StringId = 1,  KeyId = 1, LabelId = 1;
    //int NodeId = 1, PropId = 1, RelId = 1;
    static File storeDir = null;
    Config config;
    PageCache pageCache;
    String storeVersion;
    IdGeneratorFactory idGeneratorFactory;
    private MyStoreType[] myStoreTypes;
    private DynamicIntArray[] myStores = new DynamicIntArray[4];

    public MyStore(FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache,
                   IdGeneratorFactory idGeneratorFactory, String storeVersion, boolean create) throws IOException
    {
        this.layout = layout;
        this.config = config;
        this.pageCache = pageCache;
        this.storeVersion = storeVersion;
        this.idGeneratorFactory = idGeneratorFactory;

        this.storeDir = layout.databaseDirectory();
        if (!storeDir.exists()) {
            storeDir.mkdir();
            create = true;
        }
        else if (create && storeDir.listFiles().length > 0)
        {
            File[] files = storeDir.listFiles();
            for (File file: files)
                file.delete();
            storeDir.mkdir();
        }
        myStoreTypes = MyStoreType.values();
        arrayFactory = new ChunkedNumberArrayFactory( NumberArrayFactory.NO_MONITOR);
        LinkArray = new DynamicIntArray(arrayFactory, 10000, NO_ID);
        Nodes = new DynamicIntArray(arrayFactory, 10000, NO_ID);
        Rels = new DynamicIntArray(arrayFactory, 10000, NO_ID);
        Props = new DynamicIntArray(arrayFactory, 10000, NO_ID);
        Labels = new DynamicIntArray(arrayFactory, 10000, NO_ID);
        outGoing = new DynamicIntArray(arrayFactory, 10000, NO_ID);
        inComing = new DynamicIntArray(arrayFactory, 10000, NO_ID);
        myStores[MyStoreType.NODE.ordinal()] = Nodes;
        myStores[MyStoreType.RELATIONSHIP.ordinal()] = Rels;
        myStores[MyStoreType.PROPERTY.ordinal()] = Props;
        myStores[MyStoreType.LABEL.ordinal()] = Labels;

        if (!create)
        {
            readArray(Nodes, MyStoreType.NODE);
            readArray(Rels, MyStoreType.RELATIONSHIP);
            readArray(Props,MyStoreType.PROPERTY);
            readArray(Labels, MyStoreType.LABEL);

            StringStore = readMap("StringStore.mystore");
            KeyMap = readMap1("KeyMap.mystore");
            NodeMap = readMap1("NodeMap.mystore");
            KeyMapCount = readMap2("Counts.mystore");
        }
        else
        {
            StringStore = new HashMap<Integer, String>();
            KeyMap = new HashMap<String, Integer>();
            NodeMap = new HashMap<String, Integer>();
            KeyMapCount = new HashMap<Integer, Integer>();
        }

        myMetadataStore = new MyMetadataStore( fs, layout, config, pageCache, storeVersion );
        transactionMetaDataStore = myMetadataStore;
        if (create)
            createStore();
    }

    public MyMetadataStore getMetaDataStore()
    {
        return myMetadataStore;
    }

    public void saveStore()
    {
        saveArray(Nodes, MyStoreType.NODE);
        saveArray(Rels, MyStoreType.RELATIONSHIP);
        saveArray(Props,MyStoreType.PROPERTY);
        saveArray(Labels, MyStoreType.LABEL);

        saveMap(StringStore, "StringStore.mystore");
        saveMap1(KeyMap, "KeyMap.mystore");
        saveMap1(NodeMap, "NodeMap.mystore");
        saveMap2(KeyMapCount, "Counts.mystore");
    }

    public void createStore()
    {
        saveArray(Nodes, MyStoreType.NODE);
        saveArray(Rels, MyStoreType.RELATIONSHIP);
        saveArray(Props,MyStoreType.PROPERTY);
        saveArray(Labels, MyStoreType.LABEL);

        saveMap(StringStore, "StringStore.mystore");
        saveMap1(KeyMap, "KeyMap.mystore");
        saveMap1(NodeMap, "NodeMap.mystore");
        saveMap2(KeyMapCount, "Counts.mystore");
    }

    public int getNumNodes()
    { return (int)(Nodes.getEntries()/NODE_SIZE) - 1 -MyStore.START_ID; }

    public int getNumRels()
    { return (int)(Rels.getEntries()/REL_SIZE) - 1 -MyStore.START_ID; }

    public int getNumProps()
    { return (int)(Props.getEntries()/PROP_SIZE) - 1-MyStore.START_ID;}


    public long getNextId(MyStoreType storeType)
    {
        return myStoreTypes[storeType.ordinal()].getNextId();
    }
    public int getNextKeyId(KEYTYPE keyType)
    {
        int keyId = KeyId++;
        KeyMap.put(":"+keyType.name(), keyId);
        return keyId;
    }
    public int getKeyId(String value, KEYTYPE keyType, DATATYPE dataType)
    {
        int key = getKeyId(value, keyType);
        int datatype = dataType.getType() << 24;
        return (datatype | key);
    }
    public int getKeyId(String value, KEYTYPE type)
    {
        //type is: PROPERTY, LABEL, RELTYPE
        if (!KeyMap.containsKey(value+":"+type.name())) {
            int keyId = KeyId++;
            KeyMap.put(value+":"+type.name(), keyId);
        }
        return KeyMap.get(value+":"+type.name());
    }
    public String[] getKeyValue(int id )
    {
        for (String key : KeyMap.keySet())
        {
            if (KeyMap.get( key ) == id)
                return key.split(":");
        }
        return null;
    }
    private int addStringStore( String value)
    {
        int stringId = StringId++;
        StringStore.put(stringId, value);
        return stringId;
    }
    public int addProperty(String propName, String value)
    {
        return addProperty(propName, value, DATATYPE.NO_VAL, NO_ID);
    }
    public int addProperty(String propName, String value, DATATYPE type, int nextValue)
    {
        keyCount(getKeyId( propName, KEYTYPE.PROPERTY), false);
        return addProperty(getKeyId( propName, KEYTYPE.PROPERTY), value, type, nextValue);
    }
    //public int addProperty(int keyId, String value)
    //{
    //    return addProperty(keyId, value, NO_ID);
    //}

    public int addProperty(int keyId, String value, DATATYPE dataType, int nextValue)
    {
        int propId = (int) myStoreTypes[MyStoreType.PROPERTY.ordinal()].getNextId();
        int type = dataType.getType() << 24;//PropId++;
        Props.set(propId*PROP_SIZE, (type | keyId));
        Props.set(propId*PROP_SIZE + 1, addStringStore(value));
        Props.set(propId*PROP_SIZE + 2, nextValue);
        return propId;
    }

    public String getPropertyString( long reference)
    {
        String returnVal = StringStore.get( (int)reference );
        //returnVal = StringStore.get(4);
        return returnVal;
    }

    public boolean isInUse(long reference, MyStoreType storeType)
    {
        long[] cell = getCell(reference, storeType);
        if (cell[0] == NO_ID)
            return false;
        return true;
    }

    private int addLabels(int keyId, int next)
    {
        int labelId = (int) myStoreTypes[MyStoreType.LABEL.ordinal()].getNextId();//LabelId++;
        Labels.set(labelId*LABEL_SIZE, keyId);
        Labels.set(labelId*LABEL_SIZE + 1, next);
        return labelId;
    }

    private int keyCount(int key, boolean justGet)
    {
        if (justGet)
            return KeyMapCount.get(key);
        if (KeyMapCount.containsKey(key))
            KeyMapCount.put(key, (KeyMapCount.get(key) + 1));
        else
            KeyMapCount.put(key, 1);
        return KeyMapCount.get(key);
    }
    public int addNode(String idPropName, String idString, String[] label, String[] propName, String[] propValue, DATATYPE[] propType)
    {
        int labelId = NO_ID;
        int propId = NO_ID;
        if (NodeMap.containsKey( idString )) {
            System.out.println("Duplicate:["+idString+"]");
            return NodeMap.get(idString);
        }
        int nodeId = (int) myStoreTypes[MyStoreType.NODE.ordinal()].getNextId();//NodeId++;
        Nodes.set(nodeId*NODE_SIZE, propId = addProperty(idPropName, idString, DATATYPE.STRING_VAL, NO_ID ));
        for (int i = 0; i < label.length; i++) {
            labelId = addLabels(getKeyId(label[i], KEYTYPE.LABEL), labelId);
            keyCount(getKeyId(label[i], KEYTYPE.LABEL), false);
        }
        Nodes.set(nodeId*NODE_SIZE+1, labelId);
        for (int i = 0; i < propName.length; i++)
            propId = addProperty(propName[i], propValue[i], propType[i], propId);
        Nodes.set(nodeId*NODE_SIZE+2, propId);
        Nodes.set(nodeId*NODE_SIZE+3, NO_ID);
        //Nodes.set(nodeId*NODE_SIZE+4, NO_ID);
        NodeMap.put( idString, nodeId);
        return nodeId;
    }
    public int[] getLabels(long nodeId)
    {
        ArrayList<Integer> labels = new ArrayList<Integer>();
        int nextLabelId = Nodes.get(nodeId*NODE_SIZE+1);
        while (nextLabelId != NO_ID)
        {
            labels.add( Labels.get( nextLabelId*LABEL_SIZE ) );
            nextLabelId = Labels.get( nextLabelId*LABEL_SIZE + 1 );
        }
        return labels.stream().mapToInt(Integer::intValue).toArray();
    }

    /*public int addNodeProps(int nodeId, String[][] props)
    {
        int propId = NO_ID;
        for (int i = 0; i < props[0].length; i++)
            propId = addProperty(props[0][i], props[1][i], propId);
        Nodes.set(nodeId*NODE_SIZE+2, NO_ID);
        return nodeId;
    }*/

    public long[] getCell(long reference, MyStoreType storeType)
    {
        long[] returnVal = new long[storeType.getCellSize()];
        for (int i = 0; i < storeType.getCellSize(); i++)
            returnVal[i] = myStores[storeType.ordinal()].get(reference*storeType.getCellSize() + i);
        return returnVal;
    }

    public long[] getNodePropIds(long nodeId)
    {
        ArrayList<Long> props = new ArrayList<Long>();
        long nextPropId = Nodes.get(nodeId*NODE_SIZE+2);
        while (nextPropId != NO_ID)
        {
            props.add( nextPropId );
            nextPropId = Props.get( nextPropId*PROP_SIZE + 2 );
        }
        return props.stream().mapToLong(Long::longValue).toArray();
    }
    public int[] getNodePropKeyIds(long nodeId)
    {
        ArrayList<Integer> props = new ArrayList<Integer>();
        long nextPropId = Nodes.get(nodeId*NODE_SIZE+2);
        while (nextPropId != NO_ID)
        {
            props.add( Props.get( nextPropId*PROP_SIZE ) );
            nextPropId = Props.get( nextPropId*PROP_SIZE + 2 );
        }
        return props.stream().mapToInt(Integer::intValue).toArray();
    }

    public long[] getNodePropStringIds(long nodeId)
    {
        ArrayList<Long> props = new ArrayList<Long>();
        long nextPropId = Nodes.get(nodeId*NODE_SIZE+2);
        while (nextPropId != NO_ID)
        {
            props.add( nextPropId*PROP_SIZE );
            nextPropId = Props.get( nextPropId*PROP_SIZE + 1 );
        }
        return props.stream().mapToLong(Long::longValue).toArray();
    }

    public int addRel( String type, String src, String dst, String[] propName, String[] propValue, DATATYPE[] propType)
    {
        int relId = (int) myStoreTypes[MyStoreType.RELATIONSHIP.ordinal()].getNextId();//RelId++;
        int propId = NO_ID;
        int relType = getKeyId( type, KEYTYPE.RELTYPE);
        keyCount(relType, false);
        Rels.set( relId*REL_SIZE, relType);
        for (int i = 0; i < propName.length; i++)
            propId = addProperty(propName[i], propValue[i], propType[i], propId);
        Rels.set(relId*REL_SIZE+1, propId);
        int srcId = NodeMap.get(src);
        int dstId = NodeMap.get(dst);
        Rels.set(relId*REL_SIZE+2, srcId);
        Rels.set(relId*REL_SIZE+3, dstId);
        Rels.set(relId*REL_SIZE+4, outGoing.get(srcId));
        outGoing.set(srcId, relId);
        Rels.set(relId*REL_SIZE+5, inComing.get(dstId));
        inComing.set(dstId, relId);
        return relId;
    }

    public long[] getNeigbors(long nodeId, boolean outgoing)
    {
        int offset = outgoing ? 3 : 4;
        long nextRelId = Nodes.get( nodeId*NODE_SIZE + offset);
        ArrayList<Long> nodes = new ArrayList<Long>();
        return null;
    }

    public void buildLinks()
    {
        int numRels = getNumRels();
        int numNodes = getNumNodes();
        for (int i = START_ID; i < numNodes+START_ID; i++)
            Nodes.set(i*NODE_SIZE+3, NO_ID);
        for (int i = START_ID; i < numRels+START_ID; i++)
        {
            int dstId = Rels.get(i*REL_SIZE+3);
            int val = Nodes.get(dstId*NODE_SIZE+3);
            Rels.set(i*REL_SIZE+5, val);
            Nodes.set(dstId*NODE_SIZE+3, i);
        }
        for (int i = START_ID; i < START_ID + 20; i++) {
            printCell(i, MyStoreType.NODE);
        }
        for (int i = START_ID; i < numNodes+START_ID; i++)
            Nodes.set(i*NODE_SIZE+3, NO_ID);
        for (int i = START_ID; i < numRels+START_ID; i++)
        {
            int srcId = Rels.get(i*REL_SIZE+2);
            int val = Nodes.get(srcId*NODE_SIZE+3);
            Rels.set(i*REL_SIZE+4, val );
            Nodes.set(srcId*NODE_SIZE+3, i);
        }
        /*for (int i = numRels+START_ID-1; i >= START_ID; i--)
        {
            int dstId = Rels.get(i*REL_SIZE+3);
            Rels.set(i*REL_SIZE+5, Nodes.get(dstId*NODE_SIZE+3));
            Nodes.set(dstId*NODE_SIZE+3, i);
        }*/
        for (int i = START_ID; i < START_ID + 20; i++) {
            printCell(i, MyStoreType.NODE);
        }
        for (int i = START_ID; i < START_ID + 20; i++) {
            printCell(i, MyStoreType.RELATIONSHIP);
        }

    }
    public void printCell(long reference, MyStoreType myStoreType)
    {
        System.out.print(myStoreType.name()+"["+reference+"][");
        for (int j = 0; j < myStoreType.getCellSize(); j++) {
            System.out.print(myStores[myStoreType.ordinal()].get(reference * myStoreType.getCellSize() + j));
            if (j < myStoreType.getCellSize()-1)
                System.out.print(", ");
        }
        System.out.println("]");

    }

    public void saveArray(DynamicIntArray ints, MyStoreType storeType) {
        FileOutputStream out = null;
        try {
            ints.set(0, (int)(ints.getEntries()-1));
            out = new FileOutputStream(this.layout.databaseDirectory() + File.separator + storeType.getStoreFileName());
            FileChannel file = out.getChannel();
            ByteBuffer buf = ByteBuffer.allocate(4 * (int)ints.getEntries());//file.map(FileChannel.MapMode.READ_WRITE, 0, 4 * ints.length);
            for (int i = 0; i < ints.getEntries(); i++) {
                buf.putInt(ints.get(i));
            }
            buf.flip();
            int numOfBytesWritten  = file.write(buf);
            System.out.println("number of bytes written [" + numOfBytesWritten+"] to [" + storeType.getStoreFileName() +"] with ["+ (ints.getEntries()-1) +"-"+ ints.get(0)+"] entries.");
            file.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

        }
    }
    public DynamicIntArray readArray(DynamicIntArray ints, MyStoreType storeType) {
        FileInputStream in = null;
        try {
            in = new FileInputStream(this.layout.databaseDirectory() + File.separator+  storeType.getStoreFileName());
            FileChannel file = in.getChannel();
            int numOfEntries = (int)file.size()/4;
            ByteBuffer buf = ByteBuffer.allocate((int)file.size());//map(FileChannel.MapMode.READ_WRITE, 0, file.size());
            file.read( buf );
            buf.flip();
            for (int i = 0; i < numOfEntries; i++) {
                ints.set(i, buf.getInt());
            }
            System.out.println("number of bytes read [" + file.size() +"] from [" + storeType.getStoreFileName()+"] with ["+ (ints.getEntries()-1) +"-"+ ints.get(0)+"] entries.");
            file.close();
            long maxId = ints.getEntries()/storeType.getCellSize();
            myStoreTypes[storeType.ordinal()].setCurrentId(maxId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
        }
        return ints;
    }
    public  void saveMap( HashMap<Integer, String> map, String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(storeDir.getAbsolutePath() + File.separator+ fileName);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(map);
            System.out.println("number of bytes written [" + fos.getChannel().size() +"] to [" + storeDir.getAbsolutePath() + File.separator+ fileName+"] with [" + map.size()+"] entries");
            oos.close();
        } catch (Exception e) {
        }
    }
    public  HashMap<Integer, String> readMap(String fileName)
    {
        HashMap<Integer, String> map = null;
        try {
            FileInputStream fis = new FileInputStream(storeDir.getAbsolutePath() + File.separator+fileName);
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashMap<Integer, String>) ois.readObject();
            System.out.println("number of bytes read [" + fis.getChannel().size() +"] from [" + storeDir.getAbsolutePath() + File.separator+ fileName+"] with [" + map.size()+"] entries");
            ois.close();
        }catch (Exception e) {
        }
        return map;
    }
    public  void saveMap1( HashMap<String, Integer> map, String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(storeDir.getAbsolutePath() + File.separator+ fileName);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(map);
            System.out.println("number of bytes written [" + fos.getChannel().size() +"] to [" + storeDir.getAbsolutePath() + File.separator+ fileName+"] with [" + map.size()+"] entries");
            oos.close();
        } catch (Exception e) {
        }
    }
    public  HashMap<String, Integer> readMap1(String fileName)
    {
        HashMap<String, Integer> map = null;
        try {
            FileInputStream fis = new FileInputStream(storeDir.getAbsolutePath() + File.separator+fileName);
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashMap<String, Integer>) ois.readObject();
            System.out.println("number of bytes read [" + fis.getChannel().size() +"] from [" + storeDir.getAbsolutePath() + File.separator+ fileName+"] with [" + map.size()+"] entries");
            ois.close();
        }catch (Exception e) {
        }
        return map;
    }

    public  void saveMap2( HashMap<Integer, Integer> map, String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(storeDir.getAbsolutePath() + File.separator+ fileName);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(map);
            System.out.println("number of bytes written [" + fos.getChannel().size() +"] to [" + storeDir.getAbsolutePath() + File.separator+ fileName+"] with [" + map.size()+"] entries");
            oos.close();
        } catch (Exception e) {
        }
    }
    public  HashMap<Integer, Integer> readMap2(String fileName)
    {
        HashMap<Integer, Integer> map = null;
        try {
            FileInputStream fis = new FileInputStream(storeDir.getAbsolutePath() + File.separator+fileName);
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashMap<Integer, Integer>) ois.readObject();
            System.out.println("number of bytes read [" + fis.getChannel().size() +"] from [" + storeDir.getAbsolutePath() + File.separator+ fileName+"] with [" + map.size()+"] entries");
            ois.close();
        }catch (Exception e) {
        }
        return map;
    }


    TransactionMetaDataStore transactionMetaDataStore = null;
    public TransactionMetaDataStore getTransactionMetaDataStore()
    {
        return transactionMetaDataStore;
    }

    public static boolean isStorePresent(FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout )
    {
        File metaDataStore = databaseLayout.metadataStore();
        if ( !fs.fileExists( metaDataStore ) )
        {
            return false;
        }
        try ( PagedFile ignore = pageCache.map( metaDataStore, MetaDataStoreCommon.getPageSize( pageCache ) ) )
        {
            return true;
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    public long[] getLabelIds()
    {
        long[] labels = new long[(int)Labels.getEntries()-1];
        for (int i = 1; i < (int)Labels.getEntries(); i++)
            labels[i] = Labels.get( i );
        return labels;
    }

    PagedFile[] pagedFile = new PagedFile[MyStoreType.values().length];

    public PageCursor openPageCursorForReading(long id, MyStoreType storeType )
    {
        try
        {
            File  storeFile = new File(storeDir.getAbsolutePath() + File.separator+ storeType.getStoreFileName());
            if (pagedFile[storeType.ordinal()] == null)
                 pagedFile[storeType.ordinal()] = pageCache.map( storeFile, MetaDataStoreCommon.filePageSize(PageCache.PAGE_SIZE, storeType.getCellSize()));
            long pageId = id * storeType.getCellSize() / PageCache.PAGE_SIZE;
            return pagedFile[storeType.ordinal()].io( pageId, PF_SHARED_READ_LOCK, TRACER_SUPPLIER.get() );
        }
        catch ( IOException e )
        {
            // TODO: think about what we really should be doing with the exception handling here...
            throw new UnderlyingStorageException( e );
        }
    }

    public void nextRecordByCursor( PageCursor cursor, MyStoreType storeType ) throws UnderlyingStorageException
    {

        if ( cursor.getCurrentPageId() < -1 )
        {
            throw new IllegalArgumentException( "Pages are assumed to be positive or -1 if not initialized" );
        }

        try
        {
            int offset = cursor.getOffset();
            long pageId = cursor.getCurrentPageId();
            if ( offset >= pagedFile[storeType.ordinal()].pageSize() || pageId < 0 )
            {
                if ( !cursor.next() )
                {
                    return;
                }
                cursor.setOffset( 0 );
            }
            //read the current value
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /*public void reset(MyStoreType storeType)
    {
        myStoreTypes[storeType.ordinal()].resetId();
    }*/

    public long getHighestPossibleIdInUse(MyStoreType storeType)
    {
        return myStoreTypes[storeType.ordinal()].getMaxId();
    }

    public List<NamedToken> getPropertyKeyTokens()
    {
        return getKeys(KEYTYPE.PROPERTY);

    }
    public List<NamedToken> getRelationshipTypeTokens()
    {
        return getKeys(KEYTYPE.RELTYPE);
    }
    public List<NamedToken> getLabelTokens()
    {
        return getKeys(KEYTYPE.LABEL);
    }
    private List<NamedToken> getKeys(KEYTYPE type)
    {
        ArrayList<NamedToken> records = new ArrayList<>();
        Iterator<String> keys = KeyMap.keySet().iterator();
        while (keys.hasNext())
        {
            String key = keys.next();
            if (key.endsWith(type.name()))
                records.add(new NamedToken(key.split(":")[0].strip(), KeyMap.get(key)));
        }
        return records;
    }

    static public DATATYPE valueType(Object value)
    {
        String strValue = null;
        if (value instanceof String)
            return DATATYPE.STRING_VAL;
        else if (value instanceof String[])
            return DATATYPE.SHORT_ARRAY;
        else if (value instanceof Byte)
            return DATATYPE.BYTE_VAL;
        else if (value instanceof DateValue)
            return DATATYPE.DATE_VAL;
        else if (value instanceof Integer)
            return DATATYPE.DATE_VAL;
        else if (value instanceof Long)
            return DATATYPE.LONG_VAL;
        else if (value instanceof int[])
            return DATATYPE.INT_ARRAY;
        else if (value instanceof long[])
            return DATATYPE.LONG_ARRAY;
        else if (value instanceof DateValue[])
            return DATATYPE.DATE_ARRAY;
        else if (value instanceof Short)
            return DATATYPE.SHORT_VAL;
        else if (value instanceof Double)
            return DATATYPE.DOUBLE_VAL;
        else if (value instanceof Boolean)
            return DATATYPE.BOOL_VAL;
        else if (value instanceof Character)
            return DATATYPE.CHAR_VAL;
        else if (value instanceof Character[])
            return DATATYPE.CHAR_ARRAY;

        return DATATYPE.NO_VAL;
    }

    static public Object valueType(String type)
    {
        return Integer.TYPE;
    }
    public static enum DATATYPE
    {
        NO_VAL(0),
        BYTE_VAL(1), BYTE_ARRAY(2),
        STRING_VAL(3), STRING_ARRAY(4),
        INT_VAL(5), INT_ARRAY(6),
        BOOL_VAL(7), BOOL_ARRAY(8),
        SHORT_VAL(9), SHORT_ARRAY(10),
        LONG_VAL(11), LONG_ARRAY(12),
        DATE_VAL(13), DATE_ARRAY(14),
        FLOAT_VAL(15), FLOAT_ARRAY(16),
        DOUBLE_VAL(17), DOUBLE_ARRAY(18),
        CHAR_VAL(19), CHAR_ARRAY(20);

        int myType;
        DATATYPE(int type)
        {
            myType = type;
        }
        public int getType()
        {
            return myType;
        }

    }

    public static short[] shortArrayFromString(String value)
    {
        String[] strs = value.split(";");
        short[] values = new short[strs.length];
        for (int i = 0; i < strs.length; i++)
            values[i] = Short.parseShort(strs[i]);
        return values;
    }

    public static long[] longArrayFromString(String value)
    {
        String[] strs = value.split(";");
        long[] values = new long[strs.length];
        for (int i = 0; i < strs.length; i++)
            values[i] = Long.parseLong(strs[i]);
        return values;
    }
    public static int[] intArrayFromString(String value)
    {
        String[] strs = value.split(";");
        int[] values = new int[strs.length];
        for (int i = 0; i < strs.length; i++)
            values[i] = Integer.parseInt(strs[i]);
        return values;
    }
    public static float[] floatArrayFromString(String value)
    {
        String[] strs = value.split(";");
        float[] values = new float[strs.length];
        for (int i = 0; i < strs.length; i++)
            values[i] = Float.parseFloat(strs[i]);
        return values;
    }
    public static double[] doubleArrayFromString(String value)
    {
        String[] strs = value.split(";");
        double[] values = new double[strs.length];
        for (int i = 0; i < strs.length; i++)
            values[i] = Double.parseDouble(strs[i]);
        return values;
    }
    public static LocalDate[] dateArrayFromString(String value)
    {
        String[] strs = value.split(";");
        LocalDate[] values = new LocalDate[strs.length];
        for (int i = 0; i < strs.length; i++)
            values[i] = LocalDate.parse(value.subSequence(0, value.length()));
        return values;
    }
    /*
    public static public String StringFromValue( Object value, Object inStrValue)
    {
        String strValue = null;
        try {
            if (value instanceof String)
                strValue = DATATYPE.STRING_VAL.getType() + value;
            else if (value instanceof String[]) {
                strValue = DATATYPE.STRING_ARRAY.getType();
                for (String str : (String[]) value)
                    strValue += str + ";";
            } else if (value instanceof Byte)
                strValue = DATATYPE.BYTE_VAL.getType() + value.toString();
            else if (value instanceof DateValue)
                strValue = DATATYPE.DATE_VAL.getType() + value.toString();
            else if (value instanceof Integer)
                strValue = DATATYPE.INT_VAL.getType() + value.toString();
            else if (value instanceof Long)
                strValue = DATATYPE.LONG_VAL.getType() + value.toString();
            else if (value instanceof int[]) {
                strValue = DATATYPE.INT_ARRAY.getType();
                for (int str : (int[]) value)
                    strValue += str + ";";
            } else if (value instanceof long[]) {
                strValue = DATATYPE.LONG_ARRAY.getType();
                for (long str : (long[]) value)
                    strValue += str + ";";
            } else if (value instanceof DateValue[]) {
                strValue = DATATYPE.DATE_ARRAY.getType();
                for (DateValue str : (DateValue[]) value)
                    strValue += str.toString() + ";";
            } else if (value instanceof Float[]) {
                strValue = DATATYPE.FLOAT_ARRAY.getType();
                for (Float str : (Float[]) value)
                    strValue += str.toString() + ";";
            } else if (value instanceof Short)
                strValue = DATATYPE.SHORT_VAL.getType() + value.toString();
            else if (value instanceof Double)
                strValue = DATATYPE.DATE_VAL.getType() + value.toString();
            else if (value instanceof Boolean)
                strValue = DATATYPE.BOOL_VAL.getType() + value.toString();

            String inputString = null;
            if (inStrValue instanceof String[]) {
                for (String str : (String[]) inStrValue)
                    inputString += str + ";";
            } else
                inputString = (String) inStrValue;
            if (!inputString.equals(strValue.substring(1)))
                System.out.println("Values don't match:[" + inputString + "][" + strValue + "]");
        } catch (Exception e)
        {
            System.out.println("Values don't match:[");
        }
        return strValue;
    }

    private Value readValue(String value )
    {
        String type = value.substring(0,1);
        value = value.substring(1);

        if ( type == null )
        {
            return Values.NO_VALUE;
        }
        switch ( type )
        {
            case DATATYPE.BOOL_VAL.getType():
                return Values.booleanValue( Extractors.extractBooleanFromString( value ) );//readBoolean();
            case DATATYPE.BYTE_VAL.getType():
                return readByte();
            case DATATYPE.SHORT_VAL.getType():
                return Values.shortValue( (short)Extractors.extractLongfromString( value ) );
            case DATATYPE.INT_VAL.getType():
                return Values.intValue( (int)Extractors.extractLongfromString( value ) );;
            case DATATYPE.LONG_VAL.getType():
                return Values.stringValue( value );
            //return string( this, reference, stringPage );
            case DATATYPE.FLOAT_VAL.getType():
                return Values.floatValue( Float.parseFloat( value ) );
            case DATATYPE.DOUBLE_VAL.getType():
                return Values.doubleValue(Double.parseDouble( value ));
            case CHAR:
                return Values.charValue( value.toCharArray()[0]);
            case DATATYPE.STRING_VAL.getType():
                return Values.stringValue( value );;
            case SHORT_ARRAY:
                return Values.;
            case STRING:
                return Values.stringValue( value );;
            case ARRAY:
                return readLongArray();
            case GEOMETRY:
                return geometryValue();
            case TEMPORAL:
                return temporalValue();
            default:
                throw new IllegalStateException( "Unsupported PropertyType: " + type.name() );
        }
    }
*/
    public int getNumNodes(int labelId)
    {
        int count = 0;
        if (labelId != TokenRead.ANY_LABEL)
            count = KeyMapCount.get( labelId );
        else {
            Iterator<String> keys = KeyMap.keySet().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                if (key.endsWith(KEYTYPE.LABEL.name()))
                    count += KeyMapCount.get(KeyMap.get(key));
            }
        }
        return count;
    }
    public int getNumRels(int startLabelId, int typeId, int endLabelId)
    {
        return getNumRels();
    }

    public int getNum(KEYTYPE keyType)
    {
        int count = 0;
        Iterator<String> keys = KeyMap.keySet().iterator();
        while (keys.hasNext())
            if (keys.next().endsWith(keyType.name()))
                count++;
        return count;
    }
}
