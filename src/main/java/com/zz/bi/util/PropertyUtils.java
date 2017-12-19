package com.zz.bi.util;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtils {
    public static Properties loadProperties(String resource) {
        Properties properties = new Properties();
        try (InputStream props = Resources.getResource(resource).openStream()) {
            properties.load(props);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
