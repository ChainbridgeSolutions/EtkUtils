/**
 *
 * ETK Cache and ETK Data Dictionary helper methods
 *
 * NOTE: To RELOAD cached data, simply "clear cache" via page "DU - Page - Clear Cache"
 *       or call internal "CacheHelper.clearCache()" or "CacheHelper.clearCacheAll()" methods.
 *  
 * psmiley 06/22/2016
 **/

package gov.atf.bi.common.helper;


import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.lang.StringUtils;

import com.entellitrak.ExecutionContext;


@SuppressWarnings("unchecked")
public class CacheHelper {
    // enable/disable...
    private static boolean isEnabled = true;
    private static boolean isEtkDataDictionaryEnabled = true;

    // Max Classloader's (object's are cached per classloader)...
    private static final int MAX_CACHED_CLASSLOADERS = 5;

    // System Configuration table..
    private static final String APP_SYSCONFIG_RDO_TABLE = "T_SYSTEM_CONFIGURATION";
    private static final String KEY_SYSTEMCONFIG_MAP = "KEY_SYSTEMCONFIG_MAP";

    // keep node keys here, must be unique, expose interfaces (not these keys)
    private static final String KEY_CACHEHELPER_ROOT = "CacheHelperRoot"; // all data goes under this map
    private static final String KEY_APPDATA_NODE = "APPDATA_NODE"; // app-level data (all users)
    private static final String KEY_USERDATA_NODE = "USERDATA_NODE"; // user level data (by ETK login)
    private static final String KEY_ETK_DATADICT_NODE = "ETK_DATADICT_NODE"; // internal for Etk Object api
    private static final String KEY_ETK_CLASSLOADERS = "ETK_CLASSLOADERS"; // internal for Etk Object api
    private static final String KEY_ETK_DATAOBJ_BK = "ETK_DATAOBJ_BK"; // Data Objects mapped by Business Key
    private static final String KEY_ETK_DATAOBJ_TN = "ETK_DATAOBJ_TN"; // Data Objects mapped by Table Name

    // no instantiation...
    private CacheHelper() {
    }

    // Unload our cached data...
    static public void clearCache(ExecutionContext etk) {
        if (etk != null) {
            etk.getCache().remove(KEY_CACHEHELPER_ROOT);
        }
    }

    // Clear all cached data...
    static public void clearCacheAll(ExecutionContext etk) {
        if (etk != null) {
            etk.getCache().clearCache();
        }
    }

    // internal helper for caching objects...
    static private String getClassloaderId() {
        return "classLoader@" + String.valueOf(CacheHelper.class.getClassLoader().hashCode());
    }

    static private String getClassloaderId(Object myObj) {
        return "classLoader@" + String.valueOf(myObj.getClass().getClassLoader().hashCode());
    }

    // Access DOI by businesskey...
    static public EtkHelper.DataObjectInfo getEtkDataObjectInfo(ExecutionContext etk, String objBusinessKey) {
        return getEtkDataObjectInfo(etk, objBusinessKey, null);
    }

    // Access DOI by tablename...
    static public EtkHelper.DataObjectInfo getEtkDataObjectInfoByTableName(ExecutionContext etk, String objTableName) {
        return getEtkDataObjectInfo(etk, null, objTableName);
    }

    // Factory for Etk Data Object Props...
    static private EtkHelper.DataObjectInfo getEtkDataObjectInfo(ExecutionContext etk, String objBusinessKey, String objTableName) {
        EtkHelper.DataObjectInfo retval = null;

        if ((etk == null) || (StringUtils.isBlank(objBusinessKey) && StringUtils.isBlank(objTableName))) {
            return null;
        }

        if (getIsEtkDataDictionaryEnabled()) {
            if (StringUtils.isNotBlank(objBusinessKey)) {
                Map<String, Object> mpDataObjects = getMap(etk, KEY_ETK_DATADICT_NODE, getClassloaderId(), KEY_ETK_DATAOBJ_BK);
                retval = (EtkHelper.DataObjectInfo) MapHelper.getMapValue(mpDataObjects, objBusinessKey.toUpperCase());
            } else if (StringUtils.isNotBlank(objTableName)) {
                Map<String, Object> mpDataObjects = getMap(etk, KEY_ETK_DATADICT_NODE, getClassloaderId(), KEY_ETK_DATAOBJ_TN);
                retval = (EtkHelper.DataObjectInfo) MapHelper.getMapValue(mpDataObjects, objTableName.toUpperCase());
            }
        }

        // Not found?
        if (retval == null) {
            // load from database...
            retval = EtkHelper.getEtkDataObjectInfo(etk, objBusinessKey, objTableName);
            if (retval != null) {
                // Store...
                putEtkDataObjectInfo(etk, retval);
            }
        }
        // } else {
        // // retrieved from cache...
        // etk.getLogger().error("Cached Obj=" + retval.getClass().hashCode() + " Classloader=" + System.identityHashCode(retval.getClass().getClassLoader()));

        return retval;
    }

    // Lookup Data Element info...
    static public EtkHelper.DataElementInfo getEtkDataElementInfoByColumn(ExecutionContext etk, String tableName, String columnName) {
        EtkHelper.DataElementInfo retval = null;

        if ((etk == null) || StringUtils.isBlank(tableName) || StringUtils.isBlank(columnName)) {
            return null;
        }

        // get data object...
        EtkHelper.DataObjectInfo etkDataObjectInfo = getEtkDataObjectInfo(etk, null, tableName);
        if (etkDataObjectInfo != null) {
            // get Element...
            retval = etkDataObjectInfo.getDataElementInfoByColumnName(columnName);
        }

        return retval;
    }

    // Get Role Id from Code...
    static public Long getEtkRoleIdFromCode(ExecutionContext etk, String roleCode) {

        // Cached?

        return EtkHelper.getRoleIdFromCode(etk, roleCode);
    }

    static public boolean getIsEnabled() {
        return isEnabled;
    }

    static public boolean getIsEtkDataDictionaryEnabled() {
        return (getIsEnabled() ? isEtkDataDictionaryEnabled : false);
    }

    // Lookup helpers (w future caching)...
    static public String getLookupCodeFromId(ExecutionContext etk, String objBusinessKey, Number objTrackingId) {
        String retval = null;

        try {
            // Sanity...
            if ((etk != null) && StringUtils.isNotBlank(objBusinessKey) && (objTrackingId != null)) {
                // get data object...
                EtkHelper.DataObjectInfo doi = getEtkDataObjectInfo(etk, objBusinessKey);
                if (doi != null) {
                    // get from db...
                    retval = getLookupCodeFromIdByTable(etk, doi.getTableName(), objTrackingId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return retval;
    }

    // Return c_code value for given lookup...
    static public String getLookupCodeFromIdByTable(ExecutionContext etk, String tblName, Number trackingId) {
        // data cached?

        return EtkHelper.getDatabaseValueString(etk, tblName, "c_code", trackingId);
    }

    // Lookup by Code...
    static public Long getLookupIdFromCode(ExecutionContext etk, String objBusinessKey, String codeVal) {
        Long retval = null;

        try {
            // Sanity...
            if ((etk != null) && StringUtils.isNotBlank(objBusinessKey)) {
                // get data object...
                EtkHelper.DataObjectInfo doi = getEtkDataObjectInfo(etk, objBusinessKey);
                if (doi != null) {
                    // get from db...
                    retval = getLookupIdFromCodeByTable(etk, doi.getTableName(), codeVal);
                }
            }

        } catch (Exception ex) {
            etk.getLogger().error(ex);
        }

        return retval;
    }

    // Return id value for given lookup...
    static public Long getLookupIdFromCodeByTable(ExecutionContext etk, String tblName, String codeVal) {
        // data cached?

        return EtkHelper.getDatabaseValueLong(etk, tblName, "id", "where c_code = '" + codeVal + "'");
    }

    // internal helper...
    static private Map<String, Object> getMap(Map<String, Object> myMap, String myKey) {
        if ((myMap == null) || StringUtils.isBlank(myKey))
            return null;
        else
            return (Map<String, Object>) myMap.get(myKey);
    }

    // internal helper...
    static private Map<String, Object> getMap(ExecutionContext etk, String myNodeKey, String myMapKey) {
        Map<String, Object> mpNode = getNodeMap(etk, myNodeKey);
        if (mpNode == null) {
            return null;
        } else {
            return getMap(mpNode, myMapKey);
        }
    }

    // internal helper...
    static private Map<String, Object> getMap(ExecutionContext etk, String myNodeKey, String myMapKey, String myMapKey2) {
        Map<String, Object> mpNode = getMap(etk, myNodeKey, myMapKey);
        if (mpNode == null) {
            return null;
        } else {
            return getMap(mpNode, myMapKey2);
        }
    }

    // Get node Map (that contains other Maps)...
    static private Map<String, Object> getNodeMap(ExecutionContext etk, String mapKey) {
        Map<String, Object> mpRoot = getRootMap(etk);
        synchronized (mpRoot) {
            return (Map<String, Object>) mpRoot.get(mapKey);
        }
    }

    // Get root Map (that contains other Maps)...
    static private Map<String, Object> getRootMap(ExecutionContext etk) {
        Map<String, Object> retval = null;

        if (etk != null) {
            retval = (Map<String, Object>) etk.getCache().load(KEY_CACHEHELPER_ROOT);
        }

        if (retval == null) {
            // create...
            retval = mapFactory();
        }

        return retval;
    }

    // get Shared Map...
    static public Map<String, Object> getSharedMap(ExecutionContext etk, String myMapKey) {
        return getMap(etk, KEY_APPDATA_NODE, myMapKey);
    }

    // Retrieve 1 "simple" (string) setting...
    static public Integer getSystemConfigInteger(ExecutionContext etk, String code) {
        String retval = getSystemConfigString(etk, code);

        return (retval == null ? null : Integer.parseInt(retval));
    }

    // Retrieve 1 "simple" (string) setting...
    static public Integer getSystemConfigInteger(ExecutionContext etk, String code, int defaultValue) {
        Integer retval = getSystemConfigInteger(etk, code);

        return (retval == null ? defaultValue : retval);
    }

    // Retrieve 1 "simple" (string) setting...
    static public Long getSystemConfigLong(ExecutionContext etk, String code) {
        String retval = getSystemConfigString(etk, code);

        return (retval == null ? null : Long.parseLong(retval));
    }

    // Retrieve SystemConfig value as Map...
    static public Map<String, Object> getSystemConfigMap(ExecutionContext etk, String code) {
        Map<String, Object> retval = null;

        Object myVal = getSystemConfigObject(etk, code);
        if (myVal != null) {
            // If not Map, convert...
            if (myVal instanceof Map) {
                retval = (Map<String, Object>) myVal;
            } else {
                retval = new HashMap<String, Object>();
                retval.put("value", myVal);
            }
        }

        return retval;
    }

    // Retrieve 1 SystemConfig setting...
    static public Object getSystemConfigObject(ExecutionContext etk, String code) {
        Object retval = null;

        if ((etk != null) && StringUtils.isNotBlank(code)) {
            try {
                Map<String, Object> mpSysConfig = getSystemConfigRoot(etk);
                if (mpSysConfig != null)
                    retval = mpSysConfig.get(code);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return retval;
    }

    // get System Config...
    static private Map<String, Object> getSystemConfigRoot(ExecutionContext etk) throws Exception {
        Map<String, Object> retval = null;

        // error loop (never loops)
        do {
            // Sanity...
            if (etk == null)
                break;

            // get cached data...
            retval = getSharedMap(etk, KEY_SYSTEMCONFIG_MAP);

            // Found?
            if (retval != null)
                break;

            // Not found, load from db...
            Map<String, Object> mpSystemConfig = mapFactory();

            // Source all prefs from database (name/value pairs, assume unique names)...
            String sql = "select C_CODE code, C_VALUE value, C_VALUE2 value2, C_FILE file_id from " + APP_SYSCONFIG_RDO_TABLE + " order by C_CODE";
            List<Map<String, Object>> lstSysCfg = etk.createSQL(sql).returnEmptyResultSetAs(null).fetchList();
            if (lstSysCfg != null) {
                // iterate List and build config map...
                for (Map<String, Object> myMap : lstSysCfg) {
                    // get extra data (if any)...
                    String value2 = MapHelper.getMapValueNonNull(myMap, "value2");
                    Long file_id = MapHelper.getMapValueAsLong(myMap, "file_id");

                    // Build map for extra items...
                    if (StringUtils.isNotBlank(value2) || (file_id != null)) {
                        Map<String, Object> mpValues = mapFactory();
                        mpValues.put("value", MapHelper.getMapValueNonNull(myMap, "value"));
                        mpValues.put("value2", value2);
                        mpValues.put("file_id", file_id);
                        mpSystemConfig.put(MapHelper.getMapValueNonNull(myMap, "code"), mpValues);
                    } else {
                        // Store simple name/value pair...
                        mpSystemConfig.put(MapHelper.getMapValueNonNull(myMap, "code"), MapHelper.getMapValueNonNull(myMap, "value"));
                    }
                }
            }

            // Store in cache...
            putSharedMap(etk, KEY_SYSTEMCONFIG_MAP, mpSystemConfig);
            retval = mpSystemConfig;

            // Done (never loop)
        } while (false);

        return retval;
    }

    // Retrieve 1 "simple" (string) setting...
    static public String getSystemConfigString(ExecutionContext etk, String code) {
        return (String) getSystemConfigObject(etk, code);
    }

    // get User Map (current ETK user)...
    static public Map<String, Object> getUserMap(ExecutionContext etk, String mapKey) {
        // Get user data...
        return getMap(etk, KEY_USERDATA_NODE, EtkHelper.getCurrentUserLogin(etk), mapKey);
    }

    // create new Map (of our type)...
    static private Map<String, Object> mapFactory() {
        return new HashMap<String, Object>();
    }

    // Cache this object...
    static private void putEtkDataObjectInfo(ExecutionContext etk, EtkHelper.DataObjectInfo etkDataObjectInfo) {
        // sanity checks...
        if ((etk == null) || (etkDataObjectInfo == null) || !getIsEtkDataDictionaryEnabled())
            return;

        Map<String, Object> mpRoot = getRootMap(etk);
        synchronized (mpRoot) {
            Map<String, Object> mpNode = getMap(mpRoot, KEY_ETK_DATADICT_NODE);
            if (mpNode == null) {
                mpNode = mapFactory();
                mpRoot.put(KEY_ETK_DATADICT_NODE, mpNode);
            }

            // get classloader map...
            String classloaderId = getClassloaderId(etkDataObjectInfo);
            Map<String, Object> mpClassloader = getMap(mpNode, classloaderId);
            if (mpClassloader == null) {
                mpClassloader = mapFactory();
                mpNode.put(classloaderId, mpClassloader);

                // Cleanup older classloaders...
                trackClassloader(mpNode, classloaderId);
            }

            // Get BK map...
            Map<String, Object> mpEtkDataObjectInfoBK = getMap(mpClassloader, KEY_ETK_DATAOBJ_BK);
            if (mpEtkDataObjectInfoBK == null) {
                mpEtkDataObjectInfoBK = mapFactory();
                mpClassloader.put(KEY_ETK_DATAOBJ_BK, mpEtkDataObjectInfoBK);
            }

            // Add object by Business Key...
            mpEtkDataObjectInfoBK.put(etkDataObjectInfo.getBusinessKey().toUpperCase(), etkDataObjectInfo);

            // Get TN map...
            Map<String, Object> mpEtkDataObjectInfoTN = getMap(mpClassloader, KEY_ETK_DATAOBJ_TN);
            if (mpEtkDataObjectInfoTN == null) {
                mpEtkDataObjectInfoTN = mapFactory();
                mpClassloader.put(KEY_ETK_DATAOBJ_TN, mpEtkDataObjectInfoTN);
            }

            // Add object by Table Name...
            mpEtkDataObjectInfoTN.put(etkDataObjectInfo.getTableName().toUpperCase(), etkDataObjectInfo);

            // Save in cache...
            putRootNode(etk, mpRoot);
        }
    }

    // save root Map...
    static private void putRootNode(ExecutionContext etk, Map<String, Object> mpRoot) {
        // Store...
        if (getIsEnabled()) {
            etk.getCache().store(KEY_CACHEHELPER_ROOT, mpRoot);
        }
    }

    // store Shared map...
    static public void putSharedMap(ExecutionContext etk, String mapKey, Map<String, Object> mpShared) {
        if (StringUtils.isBlank(mapKey) || !getIsEnabled())
            return;

        Map<String, Object> mpRoot = getRootMap(etk);
        synchronized (mpRoot) {
            Map<String, Object> mpNode = getMap(mpRoot, KEY_APPDATA_NODE);
            if (mpNode == null) {
                mpNode = mapFactory();
                mpRoot.put(KEY_APPDATA_NODE, mpNode);
            }

            mpNode.put(mapKey, mpShared);

            // Save...
            putRootNode(etk, mpRoot);
        }
    }

    // store User map...
    static public void putUserMap(ExecutionContext etk, String mpKey, Map<String, Object> mpUser) {
        if (StringUtils.isBlank(mpKey) || !getIsEnabled())
            return;

        Map<String, Object> mpRoot = getRootMap(etk);
        synchronized (mpRoot) {
            Map<String, Object> mpNode = getMap(mpRoot, KEY_USERDATA_NODE);
            if (mpNode == null) {
                mpNode = mapFactory();
                mpRoot.put(KEY_USERDATA_NODE, mpNode);
            }

            mpNode.put(mpKey, mpUser);

            // Save...
            putRootNode(etk, mpRoot);
        }
    }

    static public void setIsEnabled(boolean myIsEnabled) {
        isEnabled = myIsEnabled;
    }

    // Each time code is compiled, ETK makes a new classloader object, limit object caching...
    static private void trackClassloader(Map<String, Object> mpNode, String classloaderId) {
        Map<String, Object> mpClassloaders = getMap(mpNode, KEY_ETK_CLASSLOADERS);
        if (mpClassloaders == null) {
            // Add...
            mpClassloaders = new LinkedHashMap<String, Object>();
            mpNode.put(KEY_ETK_CLASSLOADERS, mpClassloaders);
        }

        synchronized (mpClassloaders) {
            if (!mpClassloaders.containsKey(classloaderId)) {
                // Add...
                mpClassloaders.put(classloaderId, new Date());

                // remove?
                if (mpClassloaders.size() > MAX_CACHED_CLASSLOADERS) {
                    String oldestKey = mpClassloaders.entrySet().iterator().next().getKey();

                    // remove from cache...
                    mpNode.remove(classloaderId);

                    // remove from tracking...
                    mpClassloaders.remove(oldestKey);
                }
            }
        }
    }
}
