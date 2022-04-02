import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.Set;
public class test {
    public static void main(String[] args) {
        // Create a HashMap of string and int
        HashMap<String, Integer> wordFreqMap = new HashMap<>();
        // Add elements in Map
        wordFreqMap.put("this", 6);
        wordFreqMap.put("at", 10);
        wordFreqMap.put("is", 9);
        wordFreqMap.put("for", 1);
        wordFreqMap.put("the", 12);
        System.out.println("*********Map Contents ****");
        System.out.println(wordFreqMap);
        System.out.println("Remove While Iterating using EntrySet Iterator");
        System.out.println("*********Remove All Elements Whose Value is 10 ****");
        // Create a Iterator to EntrySet of HashMap
        Iterator<Entry<String, Integer>> entryIt = wordFreqMap.entrySet().iterator();
        Integer No_ele = wordFreqMap.size();
        // Iterate over all the elements
        while (entryIt.hasNext()) {
            Entry<String, Integer> entry = entryIt.next();
            entry.setValue(10);
            // Check if Value associated with Key is 10
            if (entry.getValue() == 10) {
                // Remove the element
                entryIt.remove();
                No_ele--;
            }
            System.out.println(wordFreqMap);
        }

    }
}