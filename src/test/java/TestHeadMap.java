import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TestHeadMap {
    public static void main(String[] args) {
        ConcurrentSkipListMap concurrentSkipListMap = new ConcurrentSkipListMap();
        for (int i = 0; i < 1000; i++) {
            concurrentSkipListMap.put(i,i+"");
        }
        ConcurrentNavigableMap concurrentNavigableMap = concurrentSkipListMap.headMap(500);
        concurrentNavigableMap.clear();
        System.out.println(concurrentSkipListMap.size());
    }
}
