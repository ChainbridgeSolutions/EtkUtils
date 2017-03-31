/**
 *
 * Map helper methods
 * 
 *   Make your code null-safe, type-safe and readable by accessing.comparing Map keys/values here.
 *   Use createMap() factory to standardize your Map type.
 *
 * psmiley 06/21/2016
 **/

package com.solutions.chainbridge.utilities;


import java.util.Map;
import java.util.Date;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.util.LinkedHashMap;

import org.apache.commons.lang.StringUtils;


@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapHelper {
    private MapHelper() {
    }

    // Copy Map with specified Keys (used to transform SqlFacade results into normal HashMap)
    static public Map clone(Map srcMap, String[] srcKeys) {
        Map mpRetval = null;

        if (srcMap != null) {
            mpRetval = createMap();

            // Keys specified?
            if (srcKeys == null) {
                // All keys...
                mpRetval.putAll(srcMap);
            } else {
                // for specified keys...
                for (String key : srcKeys) {
                    putMapValueSafe(mpRetval, key, srcMap.get(key));
                }
            }
        }

        return mpRetval;
    }

    // Copy One key to another...
    static public void copyKey(Map srcMap, String srcKey, String tarKey) {
        copyKey(srcMap, srcKey, srcMap, tarKey);
    }

    // Copy Value from one map to another...
    static public void copyKey(Map srcMap, String srcKey, Map tarMap) {
        copyKey(srcMap, srcKey, tarMap, srcKey);
    }

    // Copy Value from one map to another...
    static public void copyKey(Map srcMap, String srcKey, Map tarMap, String tarKey) {
        putMapValueSafe(tarMap, tarKey, getMapValue(srcMap, srcKey));
    }

    // Copy Values from one map to another...
    static public void copyKeys(Map srcMap, String[] srcKeys, Map tarMap) {

        if ((srcMap != null) && (srcKeys != null)) {
            // for specified keys...
            for (String key : srcKeys) {
                copyKey(srcMap, key, tarMap);
            }
        }
    }

    // default Map factory (use to control Map type)
    static public Map createMap() {
        return new HashMap();
    }

    // default Map factory (use to control Map type)
    static public Map createMap(Map mp) {
        return new HashMap(mp);
    }

    static public String decode(String firstVal, String compareVal, String trueVal) {
        return decode(firstVal, compareVal, trueVal, null);
    }

    static public String decode(String firstVal, String compareVal, String trueVal, String falseVal) {
        if ((firstVal == null) && (compareVal == null))
            return trueVal;
        if (((firstVal == null) && (compareVal != null)) || ((firstVal != null) && (compareVal == null)))
            return falseVal;
        // use string compare...
        return (firstVal.equalsIgnoreCase(compareVal) ? trueVal : falseVal);
    }

    // Get value (raw)...
    static public Object getMapValue(Map myMap, Object myKey) {
        Object retval = null;

        if (myKey != null) {
            retval = getMapValue(myMap, myKey.toString());
        }

        return retval;
    }

    // Get value (raw)...
    static public Object getMapValue(Map myMap, String myKey) {
        Object retval = null;

        if ((myMap != null) && (myKey != null)) {
            retval = myMap.get(myKey);
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public String getMapValue(LinkedHashMap<String, String> myMap, String myKey) {
        String retval = null;

        if ((myMap != null) && (myKey != null)) {
            retval = myMap.get(myKey);
        }

        return retval;
    }

    // Get value as BigDecimal (typically currency)
    static public BigDecimal getMapValueAsBigDecimal(Map myMap, String myKey) {
        BigDecimal retval = null;

        Object myObj = getMapValue(myMap, myKey);
        if (myObj != null) {
            retval = new BigDecimal(myObj.toString());
        }

        return retval;
    }

    // Convert to BigDecimal or returns BigDecimal(0)
    static public BigDecimal getMapValueAsBigDecimalOrZero(Map myMap, String myKey) {
        BigDecimal retval = null;

        Object myObj = getMapValue(myMap, myKey);
        if (myObj == null) {
            retval = new BigDecimal(0);
        } else {
            retval = new BigDecimal(myObj.toString());
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public Boolean getMapValueAsBoolean(Map myMap, String myKey) {
        Boolean retval = null;

        String myVal = getMapValueAsString(myMap, myKey);
        if (StringUtils.isNotBlank(myVal)) {
            if (myVal.equals("0"))
                retval = false;
            else if (myVal.equals("1"))
                retval = true;
            else
                retval = Boolean.valueOf(myVal);
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public Date getMapValueAsDate(Map myMap, String myKey) {
        Date retval = null;

        Object myVal = getMapValue(myMap, myKey);
        if ((myVal != null) && (myVal instanceof Date)) {
            retval = (Date) myVal;
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public int getMapValueAsInt(Map myMap, String myKey) {
        Integer retval = getMapValueAsInteger(myMap, myKey);

        return (retval == null ? 0 : retval.intValue());
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public Integer getMapValueAsInteger(Map myMap, String myKey) {
        Integer retval = null;

        String myVal = getMapValueAsString(myMap, myKey);
        if (StringUtils.isNotBlank(myVal)) {
            retval = Integer.valueOf(myVal);
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public List getMapValueAsList(Map myMap, String myKey) {
        List retval = null;

        Object myVal = getMapValue(myMap, myKey);
        if (myVal != null) {
            // If not List, convert to List...
            if (myVal instanceof List) {
                retval = (List) myVal;
            } else {
                retval = new ArrayList();
                retval.add(myVal);
            }
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public Long getMapValueAsLong(Map<String, Object> myMap, String myKey) {
        Long retval = null;

        String myVal = getMapValueAsString(myMap, myKey);
        if (StringUtils.isNotBlank(myVal)) {
            retval = Long.parseLong(myVal);
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public String getMapValueAsString(Map myMap, Object myKey) {
        String retval = null;

        if (myKey != null) {
            retval = getMapValueAsString(myMap, myKey.toString());
        }

        return retval;
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public String getMapValueAsString(Map myMap, String myKey) {
        String retval = null;

        Object myObj = getMapValue(myMap, myKey);
        if (myObj != null) {
            retval = myObj.toString();
        }

        return retval;
    }

    // Get value from Map without Null...
    static public String getMapValueNonNull(Map myMap, Object myKey) {
        String retval = "";

        if (myKey != null) {
            retval = getMapValueNonNull(myMap, myKey.toString());
        }

        return retval;
    }

    // Get value from Map without Null...
    static public String getMapValueNonNull(Map myMap, String myKey) {
        String retval = getMapValueAsString(myMap, myKey);
        if (retval == null)
            retval = "";

        return retval;
    }

    // Is Map entry there?
    static public boolean isKeySet(Map myMap, String myKey) {
        boolean retval = false;

        if ((myMap != null) && (myKey != null)) {
            retval = myMap.containsKey(myKey);
        }

        return retval;
    }

    // Simple Long compare...
    static public boolean isValueEqualLong(Map myMap, String myKey, Long myValue2) {
        boolean retval = false;

        Long myValue = getMapValueAsLong(myMap, myKey);

        if ((myValue == null) && (myValue2 == null)) {
            return true;
        } else if ((myValue != null) && (myValue2 != null)) {
            retval = (myValue.compareTo(myValue2) == 0);
        }

        return retval;
    }

    // Simple String compare...
    static public boolean isValueEqualString(Map myMap, String myKey, String myValue) {
        boolean retval = false;

        if (StringUtils.isEmpty(myValue)) {
            // Is other value also empty?
            retval = StringUtils.isEmpty(getMapValueAsString(myMap, myKey));
        } else {
            retval = myValue.equalsIgnoreCase(getMapValueAsString(myMap, myKey));
        }

        return retval;
    }

    // Simple String compare...
    static public boolean isValueEqualValue(Map myMap, String myKey, String myKey2) {
        boolean retval = false;

        // If each key is set...
        if (isKeySet(myMap, myKey) && isKeySet(myMap, myKey2)) {
            // Check via String compare...
            retval = isValueEqualString(myMap, myKey, getMapValueAsString(myMap, myKey2));
        }

        return retval;
    }

    // Same Key in 2 Maps...
    static public boolean isValueEqualValue(Map myMap, String myKey, Map myMap2) {
        return isValueEqualValue(myMap, myKey, myMap2, myKey);
    }

    // Same Key in 2 Maps...
    static public boolean isValueEqualValue(Map myMap, String myKey, Map myMap2, String myKey2) {
        boolean retval = false;

        // If each key is set...
        if (isKeySet(myMap, myKey) && isKeySet(myMap2, myKey2)) {
            // Check via String compare...
            retval = isValueEqualString(myMap, myKey, getMapValueAsString(myMap2, myKey2));
        }

        return retval;
    }

    // Is Map Value in the array of values...
    static public boolean isValueInArray(Map myMap, String myKey, String[] valueArray) {
        boolean retval = false;

        if (valueArray != null) {
            String myValue = getMapValueAsString(myMap, myKey);

            // Loop the value set...
            for (String myValue2 : valueArray) {
                if (StringUtils.equalsIgnoreCase(myValue, myValue2)) {
                    retval = true;
                    break;
                }
            }
        }

        // Loop
        return retval;
    }

    // Is Map entry there and Value non-null/empty
    static public boolean isValueSet(Map myMap, String myKey) {
        return StringUtils.isNotEmpty(getMapValueAsString(myMap, myKey));
    }

    // Standard Map factory...
    static public Map<String, Object> mapFactory() {
        return new HashMap<String, Object>();
    }

    // Get value from Map (safe, can be called with Sql Facade result)...
    static public void putMapValueSafe(Map myMap, String myKey, Object myVal) {
        if ((myMap != null) && (myKey != null)) {
            myMap.put(myKey, myVal);
        }
    }
}
