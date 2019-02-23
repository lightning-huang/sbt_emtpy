package parallex;

import basic.AsciiUtil;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParellexIniReader {
    public final static String ENV_NAME = "APP_ENV";

    public static void init() throws IOException {
        DataPathConfiguration = new ParellexIniReader("data_dependent.ini");
    }

    public static ParellexIniReader DataPathConfiguration;

    public Boolean getBoolean(String key){
        String raw = this.getProperty(key);
        return Boolean.parseBoolean(raw);
    }
    public String getProperty(String key) {
        String env = System.getProperty(ENV_NAME);

        if (!AsciiUtil.isNullOrEmpty(env)) {
            if (AllEntires.containsKey(key + "$" + env)) {
                return AllEntires.get(key + "$" + env);
            }else if (AllEntires.containsKey(key)){
                return AllEntires.get(key);
            }
        }

        return null;
    }

    private ParellexIniReader(String fileName) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(fileName));
        AllEntires = new HashMap<String, String>();
        for (String line : lines) {
            String buf = line.trim();

            if (buf.startsWith("[") || buf.startsWith("#") || buf.startsWith(";")) {
                continue;
            }

            String[] tokens = line.split("=", 2);
            String key = tokens[0].trim();
            String value = tokens[1].trim();
            AllEntires.put(key, value);
        }
    }

    private Map<String, String> AllEntires;


}
