package io.confluent.migration.utils;

import java.io.*;
import java.util.Properties;

public class KafkaUtils {

    private static Properties sourceClusterConfigs = new Properties();
    private static Properties destClusterConfigs = new Properties();


    public static Properties getSourceClusterConfigs(String absolutePath) {
        if (sourceClusterConfigs.isEmpty()) {

            sourceClusterConfigs = fileProperties(absolutePath);
        }

        return sourceClusterConfigs;
    }

    public static Properties getDestinationClusterConfigs(String absolutePath) {
        if (destClusterConfigs.isEmpty()) {


                destClusterConfigs = fileProperties(absolutePath);

        }
        return destClusterConfigs;
    }

    private static Properties  fileProperties(String absolutePath){
        Properties prop = new Properties();
        try (InputStream input = new FileInputStream(absolutePath)) {


            // load a properties file
            prop.load(input);


        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

}
