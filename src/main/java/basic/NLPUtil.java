package basic;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

public class NLPUtil {
 public static void main(String[] args) {
     Segment segment = HanLP.newSegment().enableNameRecognize(true);
     String[] testCase =new String[]{"小v搜集有关的歌曲", "小爱搜集有关的歌曲", "小度搜集有关的歌曲", "唱一首肉麻情歌", "小泉搜集有关的歌曲"};
     System.out.println(HanLP.Config.CoreDictionaryPath);
     for (String sentence : testCase)
     {
         List<Term> termList = segment.seg(sentence);
         System.out.println(termList);
     }
 }
}
