package com.vivo.arcane.earth.wordrepo;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TryBloomFilter {

    private static BloomFilter<String> newBloomFilter(int expectedInserts) {
        return BloomFilter.create(Funnels.unencodedCharsFunnel(), expectedInserts, 0.0000001d);
    }

    private static final int THRESHOLD = 10000;
    private static final int BASE = 10000;

    public static void main(String[] args) throws Exception {
        String metaFile = args[0];
        String dataFile = args[1];
        List<String> metaData = Files.readAllLines(Paths.get(metaFile));
        Map<String, Integer> cata2Inserts = new HashMap<>();
        Map<String, BloomFilter<String>> cata2BloomFilter = new HashMap<>();
        Map<String, List<String>> cata2SortedArray = new HashMap<>();

        // consume the meta data
        // only the row count > threshold we'll make the bloomfilter, otherwise we use sorted array
        for (String line : metaData) {
            if (line != null && !line.trim().equals("")) {
                String[] tokens = line.split("\t");
                double rowCount = Double.parseDouble(tokens[1]);
                double value = Math.ceil(rowCount / BASE) * BASE;

                if (rowCount > THRESHOLD) {
                    cata2Inserts.put(tokens[0], (int) value);
                } else {
                    cata2Inserts.put(tokens[0], -1);
                }
            }
        }

        String buf = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile), "UTF-8"));
        int processLine = 0;

        while ((buf = reader.readLine()) != null) {
            if (buf != null && !buf.trim().equals("")) {
                String[] tokens = buf.split(",");
                if (tokens.length > 2) {
                    processLine++;
                    if (processLine % 1000 == 0) {
                        System.out.println("Processing Line " + processLine);
                    }

                    boolean recorded = cata2Inserts.containsKey(tokens[0]);

                    if (!recorded) {
                        System.out.println(tokens[0] + " is not recorded");
                        continue;
                    }

                    int inserts = cata2Inserts.get(tokens[0]);
                    String parsedTerm = parseToken(tokens[2].toLowerCase());

                    if (parsedTerm == null) {
                        System.out.println("ignore empty value");
                        continue;
                    }

                    if (inserts > 0) {
                        if (!cata2BloomFilter.containsKey(tokens[0])) {
                            cata2BloomFilter.put(tokens[0], newBloomFilter(inserts));
                        }

                        if (!cata2BloomFilter.get(tokens[0]).mightContain(parsedTerm)) {
                            cata2BloomFilter.get(tokens[0]).put(parsedTerm);
                        }
                    } else {
                        if (!cata2SortedArray.containsKey(tokens[0])) {
                            cata2SortedArray.put(tokens[0], new ArrayList<>());
                        }

                        cata2SortedArray.get(tokens[0]).add(parsedTerm);
                    }
                }
            }
        }
        reader.close();

        for (List<String> list : cata2SortedArray.values()) {
            Collections.sort(list);
        }

        System.out.println("process completed, you can check the memory");
        int mb = 1024 * 1024;

        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        System.out.println("##### Heap utilization statistics [MB] #####");

        //Print used memory
        System.out.println("Used Memory:"
                + (runtime.totalMemory() - runtime.freeMemory()) / mb);

        //Print free memory
        System.out.println("Free Memory:"
                + runtime.freeMemory() / mb);

        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);

        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);

        for (Map.Entry<String, BloomFilter<String>> entry : cata2BloomFilter.entrySet()) {
            System.out.println(String.format("%s expected FP: %s", entry.getKey(), entry.getValue().expectedFpp()));
        }


        String[] testCases = {"红豆", "遇见", "hello", "皮肤过敏", "孟庆迪", "西雅图", "thinkpad", "杜乃乔", "vivo"};

        for (String testcase : testCases) {
            System.out.println(dictLookUp(cata2SortedArray, cata2BloomFilter, testcase));
        }

        System.out.println("Persist the model");
        String arrayModelFile = "arraymodel.jobj";
        String bloomFilterFile = "bloomfilter.jobj";
        ObjectOutputStream ostreamForArray = new ObjectOutputStream(new FileOutputStream(arrayModelFile));
        OutputStream ostreamForBloomFilter = new FileOutputStream(bloomFilterFile);
        ostreamForArray.writeObject(cata2SortedArray);
        cata2BloomFilter.values().iterator().next().writeTo(ostreamForBloomFilter);
        ostreamForArray.close();
        ostreamForBloomFilter.close();
        System.in.read();
    }

    private static final int PADDING_LENGTH = 50;

    private static String fillSpace(String raw) {
        if (raw.length() > PADDING_LENGTH) {
            return raw;
        } else {
            char[] magicChars = new char[]{' ',
                    't', 'h', 'e', ' ',
                    'l', 'i', 'g', 'h', 't', ' ',
                    's', 'h', 'a', 'l', 'l', ' ',
                    'b', 'e', ' ',
                    'w', 'i', 't', 'h', ' ',
                    'y', 'o', 'u'};
            StringBuilder sb = new StringBuilder();
            sb.append(raw);
            for (int i = 0; i < PADDING_LENGTH - raw.length(); ++i) {
                sb.append(magicChars[i % magicChars.length]);
            }
            return sb.toString();
        }
    }

    private static String parseToken(String token) {
        //keyword:红豆红,remark:
        String result = null;


        if (token != null && token.length() > 8) {
            result = fillSpace(token.substring(8));
        }

        return result;
    }

    private static String dictLookUp(Map<String, List<String>> cata2SortedArray, Map<String, BloomFilter<String>> cata2BloomFilter, String key) {
        StringBuilder sb = new StringBuilder();
        key = fillSpace(key);
        boolean hit = false;
        sb.append(key);
        if (cata2SortedArray != null) {
            for (Map.Entry<String, List<String>> entry : cata2SortedArray.entrySet()) {
                List<String> list = entry.getValue();

                if (list != null && list.size() > 0) {
                    int index = Collections.binarySearch(list, key);

                    if (index >= 0) {
                        hit = true;
                        sb.append("[" + entry.getKey() + "]");
                    }
                }
            }
        }

        if (cata2BloomFilter != null) {
            for (Map.Entry<String, BloomFilter<String>> entry : cata2BloomFilter.entrySet()) {
                BloomFilter<String> filter = entry.getValue();

                if (filter != null) {
                    boolean contains = filter.mightContain(key);

                    if (contains) {
                        hit = true;
                        sb.append("[" + entry.getKey() + "]");
                    }
                }
            }
        }

        if (!hit) {
            sb.append("[UNKNOWN]");
        }

        return sb.toString();
    }
}
