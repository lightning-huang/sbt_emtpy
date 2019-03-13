import org.junit.Test;

import java.util.Arrays;

public class ArbitrateTest {
    @Test
    public void Test1(){
        String[] xx = new String[]{"kkk","aaa","bbb","ccc","DDD"};
        Arrays.sort(xx);
        System.out.println(Arrays.binarySearch(xx, "ccc"));

    }
}
