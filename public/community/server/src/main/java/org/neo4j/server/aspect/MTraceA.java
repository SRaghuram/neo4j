package org.neo4j.server.aspect;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;

@Aspect
public class MTraceA {
    private HashMap<String, Stack<String>> sfrsStackMap = new HashMap<String, Stack<String>>();
    private HashMap<String, Integer> fullStackMap = new HashMap<String, Integer>();
    private int STACKDEPTH = 200;
    @SuppressWarnings("unchecked")
    private Stack<String>[] fullStack = getStackArray(STACKDEPTH);
    private int nextStackIndex = 0;
    private HashSet<String> duplicate = new HashSet<String>();
    private HashMap<String, Integer> dupMap = new HashMap<String, Integer>();
    private HashSet<String> duplicate1 = new HashSet<String>();
    private HashMap<String, Integer> deepCountMap = new HashMap<String, Integer>();
    private HashMap<String, Long> duplicateCountMap = new HashMap<String, Long>();
    private String stringBufferEnter = null, stringBufferExit = null;
    long startTime = System.currentTimeMillis(), prevTime;
    private int MaxSize = 10, MinSize = 5;
    private int printInterval = 100000;
    @SuppressWarnings("unchecked")
    private Stack<String>[] InScope = getStackArray(STACKDEPTH);
    private boolean printDuplicateEntryExit = false, printInsideScope = true;
    //private int[] insideScopeCount = new int[]{0,0,0,0,0,0,0,0,0,0};
    private int countEntryCalls = 0;
    private int enterCount = 0, exitCount = 0;
    private long last = 0;
    private HashSet<String> MyStoreFileset, utilFileSet, RecordEngineFileset, FrekiEngineFileset, GDSFilesCore, GDSFileKernelAdapter;
    private ArrayList<HashSet<String>> trackerFileSet;
    private HashSet<String> CheckingFileSet = null;
    private String[] noPrintMethods = new String[]{"getLastClosedTransaction", "getLastCommittedTransaction", "checkPointIfNeeded", "TransactionMetrics",
    "propertyKey", "Reference(",  "Cursor.next", "runAll", "close", "countsForNode"};

    private String checkFileName = "/Users/sraghuram/myws/gds-projects/neo4j/public/community/server/src/main/java/org/neo4j/server/aspect/checkfile";
    public MTraceA() throws IOException
    {
        printMessage("Testing MTraceAgds");
        MyStoreFileset = getClassFileSet(new File("/Users/sraghuram/myws/gds-projects/neo4j/public/community/freki-storage-engine/src/main/java"));
        FrekiEngineFileset = getClassFileSet(new File("/Users/sraghuram/myws/gds-projects/neo4j/public/community/freki-storage-engine/src/main/java"));
        RecordEngineFileset = getClassFileSet(new File("/Users/sraghuram/myws/gds-projects/neo4j/private/enterprise/record-storage-engine/src/main/java"));
        GDSFilesCore = getClassFileSet(new File("/Users/sraghuram/myws/gds-projects/graph-analytics/public/core/src/main/java"));
        GDSFileKernelAdapter = getClassFileSet(new File("/Users/sraghuram/myws/gds-projects/graph-analytics/public/compatibility/common/neo4j-kernel-adapter/src/main/java"),
                new File("/Users/sraghuram/myws/gds-projects/graph-analytics/public/compatibility/4.2/neo4j-kernel-adapter/src/main/java"));
        trackerFileSet = new ArrayList<HashSet<String>>();
        //trackerFileSet.add(MyStoreFileset);
        //trackerFileSet.add(RecordEngineFileset);
        trackerFileSet.add(GDSFilesCore);
        //trackerFileSet.add(GDSFileKernelAdapter);
        //utilFileSet = getClassFileSet(
        //        new File("/Users/sraghuram/myws/gds-projects/neo4j/public/community/import-util/src/main/java"),
        //        new File("/Users/sraghuram/myws/gds-projects/neo4j/public/community/collections/src/main/java"),
        //        new File("/Users/sraghuram/myws/gds-projects/neo4j/public/community/values/src/main/java"),
        //        new File("/Users/sraghuram/myws/gds-projects/neo4j/public/community/random-values/src/main/java"));
                //new File("/Users/sraghuram/myws/gds-projects/neo4j/public/community/xxx/src/main/java"));
    }

    @Pointcut("(execution(* org.neo4j.graphalgo.core.huge..*(..)) || " +
            "execution(* org.neo4j.graphalgo.core.loading..*(..)) || " +
            "execution(* org.neo4j.graphalgo.core.huge.HugeGraph.*(..)) || " +
            "execution(* org.neo4j.graphalgo.core.loading.construction..*(..)) || " +
            "execution(* org.neo4j.graphalgo.core.loading.nodeproperties..*(..)) || " +
            "execution(* org.neo4j.graphalgo.api..*(..)) || " +
            "execution(* org.neo4j.graphalgo.catalog..*(..)) ||" +
            "execution(* org.neo4j.graphalgo.core.huge..*(..)) || " +
            "execution(* org.neo4j.graphalgo.compat..*(..)) ) " +
            "&& !cflow(within(org.neo4j.server.aspect.MTraceA))")


 /*
    @Pointcut("(execution(* org.neo4j.internal.batchimport..*(..)) || " +
            "execution(* com.neo4j.kernel.impl.store.format.highlimit..*(..)) || " +
            "execution(* org.neo4j.tooling..*(..)) || " +
            "execution(* org.neo4j.io.layout..*(..)) || " +
            "execution(* org.neo4j.internal.batchimport.io..*(..)) || " +
            "execution(* org.neo4j.internal.recordstorage..*(..)) || " +
            "execution(* org.neo4j.kernel.impl.store..*(..)) || " +
            "execution(* org.neo4j.kernel.impl.storemigration..*(..)) || " +
            "execution(* org.neo4j.kernel.impl.store.format..*(..)) || " +
            "execution(* org.neo4j.kernel.impl.newapi..*(..)) ||" +
            "execution(* org.neo4j.kernel.impl.store.counts..*(..)) ||" +
            "execution(* org.neo4j.internal.freki..*(..)) ) " +
            "&& !cflow(within(org.neo4j.server.aspect.MTraceA))")
  */
    public void traceMethods(){
        //
    }

    @Before("traceMethods()")
    public void beforeMethod_new()
    {
        try {
            String threadName = Thread.currentThread().getName().split(" ")[0];
            long threadId = Thread.currentThread().getId();
            if (enterCount++ % 1000000 == 0) {
                printMessage("EnterCount:" + enterCount +":"+exitCount + "-" + (System.currentTimeMillis() - last));
                last = System.currentTimeMillis();
            }

            int stackIndex = 0;
            if (!fullStackMap.containsKey(threadName))
            {
                stackIndex = nextStackIndex;
                //printMessage("Allocating stack to thread:"+ threadName +"["+stackIndex+"]");
                fullStackMap.put(threadName, nextStackIndex++);
            }
            stackIndex = fullStackMap.get(threadName);
            WhereInCode where = processStack(stackIndex);
            switch (where) {

                case OutsideScope:
                    fullStack[stackIndex].empty();
                    break;
                case ExitingScope:
                    fullStack[stackIndex].empty();
                    break;
                case EnteringScope :
                    if (!printDuplicateEntryExit && isItDuplicate1( dupMap, previousEntry[stackIndex]+"=="+currentEntry[stackIndex]))
                    {
                        long count = 0;
                        if (duplicateCountMap.get(threadName) != null)
                            count = duplicateCountMap.get(threadName);
                        duplicateCountMap.put(threadName, ++count);
                        //if (count % printInterval == 0)
                        //    printMessage("["+threadName+"] -- DuplicateCount:["+count+"] Time:["+(System.currentTimeMillis()-startTime)+" ms]");
                    }
                    else {
                        String indent = "";
                        int dupCount = dupMap.get(previousEntry[stackIndex]+"=="+currentEntry[stackIndex]);
                        for (int i = 0; i < InScope[stackIndex].size(); i++)
                            indent += "\t";
                        printMessage(++countEntryCalls +":["+dupCount+"]"+indent + "[" + Thread.currentThread().getId() + ":" + threadName + "] " + previousEntry[stackIndex] + " \n\t\t\t "+
                                (InScope[stackIndex].size() > 0 ? "["+InScope[stackIndex].size()+"]" : "") + "===>[enter]" + "[" +
                                threadName + "] " + currentEntry[stackIndex] +" [returns: "+returnValue[stackIndex]+"]");

                        if (sfrsStackMap.get(threadName) == null)
                            sfrsStackMap.put(threadName, new Stack<String>());
                        sfrsStackMap.get(threadName).push(stripLineNo(currentEntry[stackIndex]));
                        deepCountMap.put(threadName, 0);
                    }
                    fullStack[stackIndex].empty();
                    InScope[stackIndex].push(currentEntry[stackIndex]+" :: "+previousEntry[stackIndex]);
                    break;
                case InsideScope:
                    int count = 0;
                    if (deepCountMap.get(threadName) != null)
                        count = deepCountMap.get(threadName);
                    deepCountMap.put(threadName, ++count);

                    ((Stack<String>)fullStack[stackIndex]).push(stripLineNo(currentEntry[stackIndex]));
                    if (printInsideScope && !isItDuplicate( duplicate, currentEntry[stackIndex]) && !isItDuplicate( duplicate1, currentEntry[stackIndex])) {
                        System.out.print("\t\t\t\t");
                        for (int i = 0; i < fullStack[stackIndex].size(); i++)
                            System.out.print("=>");
                        printMessage("[" + fullStack[stackIndex].size() + ":" + count + "] " + currentEntry[stackIndex]);
                    }
                    /*if (count % printInterval == 0) {
                        long curTime = System.currentTimeMillis();
                        printMessage("[" + threadName + "] -- DeepCount:[" + count + "] Time:[" + (prevTime - startTime) +":"+(curTime-prevTime)+ " ms]");
                        prevTime = curTime;
                    }*/
                    break;
                default:
            }
        } catch (Exception e)
        {
            printMessage("Error in BeforeMethod:"+e.getMessage());
        }
    }


    @After("traceMethods()")
    public void afterMethod_new() {
        try {
            String threadName = Thread.currentThread().getName().split(" ")[0];
            int stackIndex = fullStackMap.get(threadName);
            if (exitCount++ % 1000000 == 0) {
                printMessage("ExitCount:[" + enterCount + "]:[" + exitCount + ";-" + (System.currentTimeMillis() - last));
                last = System.currentTimeMillis();
            }
            WhereInCode where = processStack(stackIndex);
            switch (where) {
                case OutsideScope:
                case ExitingScope:
                    break;
                case EnteringScope:
                    if (sfrsStackMap.get(threadName) != null && !sfrsStackMap.get(threadName).empty() &&
                            sfrsStackMap.get(threadName).peek().equalsIgnoreCase(stripLineNo(currentEntry[stackIndex]))) {
                        sfrsStackMap.get(threadName).pop();
                        printMessage("\t\t\t " + ((InScope[stackIndex].size() - 1) > 0 ? "[" + (InScope[stackIndex].size() - 1) + "]" : "") + "===>[exit]-[" + threadName + "] " + currentEntry[stackIndex]);
                        deepCountMap.put(threadName, 0);
                        duplicate1.clear();
                    }
                    if (InScope[stackIndex].empty())
                        printMessage("ERROROORRR:[" + enterCount + "][" + exitCount + "]");
                    else
                        InScope[stackIndex].pop();
                    break;
                case InsideScope:
                    if (!fullStack[stackIndex].empty()) {
                        int location = fullStack[stackIndex].search(stripLineNo(currentEntry[stackIndex]));
                        if (location != -1)
                            for (int i = 0; i < location; i++)
                                fullStack[stackIndex].pop();
                    }
                    break;
                default:
            }
        } catch (Exception e)
        {
            printMessage("Error in afterMethod:"+e.getMessage());
        }
    }

    private boolean isNoPrint(String str)
    {
        int end = noPrintMethods.length;
        if (trackerIndex == 0)
            end = 4;
        for (int i = 0; i < end; i++) {
            if (str.contains(noPrintMethods[i]))
                return true;
        }
        return false;
    }
    private void printMessage(String str)
    {
        if (!isNoPrint(str))
            System.out.println(str);
    }
    private Stack[] getStackArray(int size)
    {
        Stack[] returnVal = new Stack[size];
        for (int i = 0 ; i < size; i++)
            returnVal[i] = new Stack<String>();
        return returnVal;
    }

    private String getThreadId()
    {
        long id = Thread.currentThread().getId();
        String name = Thread.currentThread().getName();
        return null;
    }
    private boolean isItDuplicate(HashSet<String> duplicate, String str)
    {
        if (allowDuplicates == 1)
            return false;
        if (duplicate.contains(str))
            return true;
        duplicate.add(str);
        return false;
    }
    private boolean isItDuplicate1(HashMap<String, Integer> duplicate, String str)
    {
        try {
            if (duplicate.containsKey(str)) {
                duplicate.put(str, (duplicate.get(str) + 1));
                if (allowDuplicates == 1)
                    return false;
                return true;
            }
            duplicate.put(str, 1);
        } catch (Exception e)
        {
            System.out.println("Error in isItDuplicate:"+ e.getMessage());
        }
        return false;
    }

    private String stripLineNo(String in)
    {
        if (in.indexOf(":") == -1)
            return in;
        return in.substring(0, in.indexOf(":"));
    }

    public enum WhereInCode
    {
        EnteringScope,
        ExitingScope,
        InsideScope,
        OutsideScope
    }

    private String[] currentEntry = new String[STACKDEPTH], previousEntry = new String[STACKDEPTH], returnValue = new String[STACKDEPTH];
    long previousCheckTime = 0;
    int trackerIndex = 0;
    int allowDuplicates = 1;
    String previousCmd = "0";
    private StackWalker.StackFrame currentScopeEntryFrame;

    private boolean searchStack(List<StackWalker.StackFrame> stack, String forFrame )
    {
        Iterator<StackWalker.StackFrame> it = stack.iterator();
        while (it.hasNext())
        {
            if (it.next().toString().equals(forFrame))
                return true;
        }
        return false;
    }

    private  WhereInCode processStack(int stackIndex)
    {
        String str1, str2;
        List<StackWalker.StackFrame> stack = StackWalker.getInstance(RETAIN_CLASS_REFERENCE).walk(s ->
                s.limit(MinSize).collect(Collectors.toList()));
        int curIndex = 2, prevIndex = 3;
        if (stack.size() <= 3)
        {
            //printMessage("Stack not large enough");
            return WhereInCode.ExitingScope;
        }
        StackWalker.StackFrame curFrame = stack.get(curIndex);
        StackWalker.StackFrame prevFrame = stack.get(prevIndex);
        currentEntry[stackIndex] = curFrame.toString();
        previousEntry[stackIndex] = prevFrame.toString();
        returnValue[stackIndex] = curFrame.getMethodType().returnType().getTypeName();
        try {

            if (curFrame.getMethodName().equals(prevFrame.getMethodName()) &&
                    currentEntry[stackIndex].substring(0, currentEntry[stackIndex].indexOf(":")).equalsIgnoreCase(previousEntry[stackIndex].substring(0, previousEntry[stackIndex].indexOf(":")))) {
                prevIndex = prevIndex + 1;
                previousEntry[stackIndex] = stack.get(prevIndex).toString();
            }
            /*if (stack.get(curIndex).getMethodName().equals(stack.get(prevIndex).getMethodName()) &&
                    stack.get(curIndex).toString().substring(0, stack.get(curIndex).toString().indexOf(":")).equalsIgnoreCase(stack.get(prevIndex).toString().substring(0, stack.get(prevIndex).toString().indexOf(":")))) {
                prevIndex = prevIndex + 1;
            }*/
        } catch (Exception e)
        {
            printMessage("ERROR:"+e.getMessage()+"["+currentEntry[stackIndex]+"]["+previousEntry[stackIndex]+"]");
            prevIndex = prevIndex + 1;
            previousEntry[stackIndex] = stack.get(prevIndex).toString();
        }
        int location = 0;
        str1="0000"; str2="1111";
        try
        {
            str1 = currentEntry[stackIndex].substring(0, currentEntry[stackIndex].indexOf("("));
            str1 = str1.substring(0, str1.lastIndexOf("."));
            location = 1;
            if (str1.contains("$"))
                str1 = str1.substring(0, str1.indexOf("$"));
            location = 2;
            str2 = previousEntry[stackIndex].substring(0, previousEntry[stackIndex].indexOf("("));
            str2 = str2.substring(0, str2.lastIndexOf("."));
            location = 3;
            if (str2.contains("$"))
                str2 = str2.substring(0, str2.indexOf("$"));
        } catch (Exception e)
        {
            printMessage("ProcessStack Error:["+location+"]["+str1+"]["+str2+"] "+ e.getMessage());//e.printStackTrace();
        }
//------------
        if (System.currentTimeMillis() - previousCheckTime > 10000)
        {
            previousCheckTime = System.currentTimeMillis();
            File checkFile = new File(checkFileName);
            try {
                Scanner sc = new Scanner(checkFile);
                String cmd = sc.nextLine();
                if (previousCmd != null && !previousCmd.equalsIgnoreCase(cmd)) {
                    System.out.println("Tracking changed from[" + previousCmd + "] to [" + cmd + "]");
                    previousCmd = cmd;
                    String[] cmds = cmd.split(";");
                    trackerIndex = Integer.parseInt(cmds[0]);
                    if (cmds.length > 1) {
                        allowDuplicates = Integer.parseInt(cmds[1]);
                        duplicate.clear();
                        duplicate1.clear();
                    }
                }
            }catch (Exception e)
            {// do nothing
            }
        }
        boolean currentInScope  = trackerFileSet.get(trackerIndex).contains( str1 ) ? true : false;
        boolean previousInScope = trackerFileSet.get(trackerIndex).contains( str2 ) ? true : false;

        if (currentInScope && !previousInScope) {
            if (previousEntry[stackIndex].indexOf("neo4j") != -1 &&
                    !(previousEntry[stackIndex].contains(".values.") || previousEntry[stackIndex].contains(".collection."))) {
                currentScopeEntryFrame = curFrame;
                return WhereInCode.EnteringScope;
            }
            //if previous is not in neo4j module - search the stack till neo4j module is hit - it always does
            stack = StackWalker.getInstance(RETAIN_CLASS_REFERENCE).walk(s ->
                    s.filter(f -> !f.toString().startsWith("java.base")).limit(MaxSize).collect(Collectors.toList()));
            int index = 2;
            while (true) {
                String stackEntry = stack.get(index).toString();
                if (stackEntry.indexOf("neo4j") != -1) {
                    if (!(stackEntry.contains(".values.") || stackEntry.contains(".collection.")))
                        break;
                }
                if (++index >= MaxSize) {
                    MaxSize += 10;
                    stack = StackWalker.getInstance(RETAIN_CLASS_REFERENCE).walk(s ->
                            s.filter(f -> !f.toString().startsWith("java.base")).limit(MaxSize).collect(Collectors.toList()));
                }
            }
            MaxSize = 10;
            previousEntry[stackIndex] = stack.get(index).toString();
            str2 = previousEntry[stackIndex].substring(0, previousEntry[stackIndex].indexOf("("));
            str2 = str2.substring(0, str2.lastIndexOf("."));
            if (str2.contains("$"))
                str2 = str2.substring(0, str2.indexOf("$"));
            previousInScope = trackerFileSet.get(trackerIndex).contains( str2 ) ? true : false;
            if (!previousInScope) {
                currentScopeEntryFrame = curFrame;
                return WhereInCode.EnteringScope;
            }
        }
        if (currentInScope && previousInScope)
            return WhereInCode.InsideScope;

        if (!currentInScope && !previousInScope)
            return WhereInCode.OutsideScope;
        if (currentScopeEntryFrame != null && searchStack(stack, currentScopeEntryFrame.toString()))
            return WhereInCode.InsideScope;
        currentScopeEntryFrame = null;
        return WhereInCode.ExitingScope;
    }

    public static HashSet<String> getClassFileSet(File... directory) throws IOException {
        HashSet<String> filesList = new HashSet<String>();
        for (int j= 0; j < directory.length;j++) {
            File[] dirChild = directory[j].listFiles();
            if (dirChild == null)
                return null;
            for (int i = 0; i < dirChild.length; i++)
                if (dirChild[i].isDirectory())
                    filesList.addAll(getClassFileSet(dirChild[i]));
                else {
                    String path = dirChild[i].getPath().replace(".java", "");
                    path = path.contains("org") ? path.substring(path.indexOf("org")).replace("/", "."):
                            path.substring(path.indexOf("com")).replace("/", ".");
                    //if (path.contains("Record"))
                    //    printMessage("XXXXXXX-"+path);
                    filesList.add(path);
                }
        }
        return filesList;
    }

}

