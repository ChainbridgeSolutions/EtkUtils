/**
 *
 * Json Helper tools/utils
 *
 * psmiley 09/19/2016
 **/

package com.solutions.chainbridge.utilities;


import java.util.Map;
import java.util.List;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class JsonHelper {
    private JsonHelper() {
    }

    // Make java string json safe...
    public static String escapeJson(final String str) {
        if (StringUtils.isEmpty(str))
            return "";

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);

            if ((c == '\\') || (c == '"')) {
                sb.append('\\').append(c);
            } else if (c == '\b') {
                sb.append("\\b");
            } else if (c == '\t') {
                sb.append("\\t");
            } else if (c == '\n') {
                sb.append("\\n");
            } else if (c == '\f') {
                sb.append("\\f");
            } else if (c == '\r') {
                sb.append("\\r");
            } else if (c < ' ') {
                sb.append("\\u").append(String.format("%04X", (int) c));
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    // Convert Json string to List (if array)...
    public static List<Map<String, Object>> getListFromObject(final Object jsonObj) {
        // try to make a List of Map
        List<Map<String, Object>> list = null;
        if ((jsonObj != null) && (jsonObj instanceof String)) {
            try {
                // Attempt to make an array...
                list = new ObjectMapper().readValue((String) jsonObj, new TypeReference<List<Map<String, Object>>>() {
                });
            } catch (Exception e) {
                // e.printStackTrace();
            }
        }

        return list;
    }

    // Convert Json text to Map/List...
    public static Map<String, Object> getMap(final String jsonString) {

        // Get Map...
        Map<String, Object> result = null;
        try {
            // Get initial Map...
            result = new ObjectMapper().readValue(jsonString, new TypeReference<HashMap<String, Object>>() {
            });

            // Parse Arrays (if any)...
            parseArrays(result);
        } catch (Exception e) {
            // e.printStackTrace();
        }

        return result;
    }

    // Convert Json arrays to List<> (if any)...
    private static void parseArrays(final List<Map<String, Object>> lstJson) {

        if (lstJson != null) {
            // Convert arrays (if any)...
            for (Map<String, Object> myMap : lstJson) {
                parseArrays(myMap);
            }
        }
    }

    // Convert Json arrays to List<> (if any)...
    private static void parseArrays(final Map<String, Object> mpJson) {

        if (mpJson != null) {
            // Convert arrays (if any)...
            for (String myKey : mpJson.keySet()) {
                List<Map<String, Object>> myList = getListFromObject(mpJson.get(myKey));

                // Converted?
                if (myList != null) {
                    mpJson.put(myKey, myList);
                    parseArrays(myList);
                }
            }
        }
    }

}
