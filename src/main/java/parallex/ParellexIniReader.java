package parallex;

import basic.AsciiUtil;
import spire.math.All;

import javax.sound.sampled.Line;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

class LineParseResult {
    public String mainKey;
    public String env;
    public List<String> varints;
    public String value;
}

public class ParellexIniReader {
    public final static String ENV_NAME = "APP_ENV";
    public final static String KV_REGEX_SEPERATOR = "=";

    public static void init() throws IOException {
        DataPathConfiguration = new ParellexIniReader("data_dependent.ini");
    }

    public static ParellexIniReader DataPathConfiguration;

    public Boolean getBoolean(String key) {
        String raw = this.getProperty(key);
        return Boolean.parseBoolean(raw);
    }

    public String getProperty(String key, List<String> variants) {
        String env = System.getProperty(ENV_NAME);
        String result = null;

        if (AllEntires.containsKey(key)) {
            List<LineParseResult> bulk = AllEntires.get(key);
            // default behavior
            LineParseResult lastOne = bulk.get(bulk.size() - 1);
            if (AsciiUtil.isNullOrEmpty(lastOne.env) && (lastOne.varints == null || lastOne.varints.size() == 0)) {
                result = lastOne.value;
            }
            // match from top to bottom
            for (LineParseResult item : bulk) {
                boolean envMatch = AsciiUtil.isNullOrEmpty(item.env) || item.env.equals(env);
                boolean variantsMatch = false;

                if (item.varints == null || item.varints.size() == 0) {
                    variantsMatch = true;
                } else {
                    variantsMatch = variants != null && variants.containsAll(item.varints);
                }

                if (envMatch && variantsMatch) {
                    result = item.value;
                    break;
                }
            }
        }

        return result;
    }

    public String getProperty(String key) {
        return this.getProperty(key, null);
    }

    private ParellexIniReader(String fileName) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(fileName));
        AllEntires = new HashMap<>();

        for (String line : lines) {
            String buf = line.trim();

            if (buf.startsWith("[") || buf.startsWith("#") || buf.startsWith(";")) {
                continue;
            }

            LineParseResult result = parseKeyForIndexBuild(buf);

            if (!AllEntires.containsKey(result.mainKey)) {
                AllEntires.put(result.mainKey, new ArrayList<LineParseResult>());
            }

            AllEntires.get(result.mainKey).add(result);
        }
    }

    public static String PrintTest(String line) {
        LineParseResult result = parseKeyForIndexBuild(line);
        StringBuilder sb = new StringBuilder();
        sb.append(result.mainKey + "\t");
        sb.append(result.env + "\t");

        if (result.varints != null && result.varints.size() > 0) {
            sb.append(String.join(",", result.varints));
        }

        sb.append("\t");
        sb.append(result.value);
        return sb.toString();
    }

    private static LineParseResult parseKeyForIndexBuild(String line) {
        String[] tokens = line.split(KV_REGEX_SEPERATOR, 2);
        String key = tokens[0].trim();
        String value = tokens[1].trim();
        int regionStart = 0;
        // 0 means mainKey, 1 means dollarPart, 2 means variant part
        int status = 0;
        boolean dollarSeen = false;
        boolean andSeen = false;
        String mainKey = null;
        String dollarPart = null;
        List<String> variants = new ArrayList<String>();


        for (int i = 0; i < key.length(); ++i) {
            char currentChar = key.charAt(i);

            switch (currentChar) {
                case '$':
                    if (dollarSeen) {
                        // it is forbidden we have more than 2 '$'
                        throw new RuntimeException("Illigal ini line: " + line);
                    } else if (andSeen) {
                        // it is forbidden to meat '$' follows '&'
                        throw new RuntimeException("Illigal ini line: " + line);
                    } else {
                        // otherwise we can yield the main key
                        mainKey = key.substring(regionStart, i);
                    }
                    // meat '$', we should move status to dollar phase, region go next to track
                    status = 1;
                    regionStart = i + 1;
                    dollarSeen = true;
                    break;
                case '&':
                    if (dollarSeen) {
                        // if '$' seen and we meat '&', this is dollar part
                        dollarPart = key.substring(regionStart, i);
                    } else if (andSeen) {
                        // if '&' seen and we meat '&' again, aggregate the varaints
                        variants.add(key.substring(regionStart, i));
                    } else {
                        // otherwise we can yield the main key
                        mainKey = key.substring(regionStart, i);
                    }
                    // meat '&', we should move status to variant phase, region go next to track
                    status = 2;
                    regionStart = i + 1;
                    andSeen = true;
                    break;
            }
        }

        // finalize work, if still mainKey part, yield mainKey, if dollarPart, yield dollar part, or we yield last varaint part
        switch (status) {
            case 0:
                mainKey = key.substring(regionStart);
                break;
            case 1:
                dollarPart = key.substring(regionStart);
                break;
            case 2:
                variants.add(key.substring(regionStart));
                break;
        }

        LineParseResult result = new LineParseResult();
        result.mainKey = mainKey;
        result.env = dollarPart;
        result.varints = variants;
        result.value = value;
        return result;
    }

    private Map<String, List<LineParseResult>> AllEntires;

}
