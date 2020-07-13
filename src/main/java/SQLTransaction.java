import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class SQLTransaction {
    private Map<String, Stack<Integer>> itemsMap = new HashMap();
    private Map<Integer, Stack<String>> transactionsMap = new HashMap();
    private Map<String, Stack<Integer>> deletedItemsMap = new HashMap();
    private Map<Integer, Stack<String>> deletedTransactionsMap = new HashMap();
    private Set<Integer> abortedIndexes = new HashSet();
    private Integer currentIndx = 0;
    private Integer latestIndx = 0;


    public Integer get(String key) {
        if (!this.itemsMap.containsKey(key)) {
            return Integer.MIN_VALUE;
        }

        Stack<Integer> st = this.itemsMap.get(key);
        return st.isEmpty() ? Integer.MIN_VALUE : st.peek();

    }

    public void set(String key, Integer value) {
        if (this.currentIndx > 0) {
            if (!this.transactionsMap.containsKey(this.currentIndx)) {
                this.transactionsMap.put(this.currentIndx, new Stack());
            }

            this.transactionsMap.get(this.currentIndx).push(key);
        }

        if (!this.itemsMap.containsKey(key)) {
            this.itemsMap.put(key, new Stack());
        }

        this.itemsMap.get(key).push(value);
    }

    public void delete(String key) {
        if (this.currentIndx > 0) {
            this.deletedItemsMap.put(key, this.itemsMap.get(key));
            if (!this.deletedTransactionsMap.containsKey(this.currentIndx)) {
                this.deletedTransactionsMap.put(this.currentIndx, new Stack());
            }

            this.deletedTransactionsMap.get(this.currentIndx).push(key);
        }
        this.itemsMap.remove(key);
    }

    public void begin() {
        while(abortedIndexes.contains(currentIndx++));
        if (this.currentIndx > this.latestIndx) {
            this.latestIndx = this.currentIndx;
        }
    }

    public void commit() {
        if (this.currentIndx < 0) {
            throw new RuntimeException("All transactions already performed");
        }

        while(abortedIndexes.contains(currentIndx--));
    }

    public void rollback() {
        if (this.currentIndx < 0) {
            throw new RuntimeException("All transactions are already performed!");
        }

        while(this.latestIndx >= this.currentIndx) {
            if (!this.abortedIndexes.contains(this.currentIndx)) {
                this.abortedIndexes.add(this.latestIndx);
                if (this.deletedTransactionsMap.containsKey(this.latestIndx) && !this.deletedTransactionsMap.get(this.latestIndx).isEmpty()) {
                    Stack<String> dSt = this.deletedTransactionsMap.get(this.latestIndx);

                    while(!dSt.isEmpty()) {
                        String item = dSt.pop();
                        Stack<Integer> newitemStack = this.deletedItemsMap.getOrDefault(item, new Stack<Integer>());
                        Stack<Integer> exisitingItemStack = this.itemsMap.getOrDefault(item, new Stack<Integer>());
                        Stack<Integer> temp = new Stack();

                        while(!exisitingItemStack.isEmpty()) {
                            temp.push(exisitingItemStack.pop());
                        }

                        while(!temp.isEmpty()) {
                            newitemStack.push(temp.pop());
                        }

                        this.itemsMap.put(item, newitemStack);
                    }
                }

                Stack<String> tSt = this.transactionsMap.getOrDefault(latestIndx, new Stack<String>());
                while(!tSt.isEmpty()) {
                    String item = tSt.pop();
                    Stack<Integer> st = this.itemsMap.getOrDefault(item, new Stack<Integer>());
                    if(!st.isEmpty()) st.pop();
                    itemsMap.put(item, st);
                }

            }
            latestIndx--;
        }
        currentIndx--;
    }

    public static void main(String[] args) {
        SQLTransaction s = new SQLTransaction();
        s.begin();
        s.set("e", 50);
        System.out.println("e:" + s.get("e"));
        s.begin();
        s.set("f", 60);
        s.set("e", 55);
        System.out.println("f:" + s.get("f"));
        System.out.println("e:" + s.get("e"));
        s.begin();
        s.set("g", 70);
        s.set("f", 7);
        s.delete("e");
        System.out.println("g:" + s.get("g"));
        System.out.println("e:" + s.get("e"));
        s.rollback();
        System.out.println("g:" + s.get("g"));
        System.out.println("f:" + s.get("f"));
        System.out.println("e:" + s.get("e"));
        s.set("e", 58);
        s.delete("e");
        System.out.println("e:" + s.get("e"));
        s.rollback();
        System.out.println("g:" + s.get("g"));
        System.out.println("f:" + s.get("f"));
        System.out.println("e:" + s.get("e"));
        s.commit();
    }
}
