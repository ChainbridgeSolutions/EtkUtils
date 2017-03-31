/**
 *
 * ETK related helper methods.
Get data dictionary
Get user info
etc
 *
 * Originally developed on ETK 3.17.0.1
 * May need changes for other ETK versions
 *
 * psmiley 06/26/2016
 **/

package com.solutions.chainbridge.utilities;


import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import com.entellitrak.ApplicationException;
import com.entellitrak.DataAccessException;
import com.entellitrak.ExecutionContext;


public class EtkHelper {
    public static final long ETK_ADMINUSER_ID = 1l;
    public static final String ETK_ADMINUSER_LOGIN = "administrator";
    public static final String ETK_ADMINUSER_ROLE_BK = "role.administration";
    public static final String ETK_ADMINUSER_ROLE_NAME = "Administrator";

    // ETK Element data types...
    public enum DataElementType {
        TEXT(1), NUMBER(2), DATE(3), CURRENCY(4), YES_NO(5), FILE(8), STATE(9), PASSWORD(10), LONG_TEXT(11), TIMESTAMP(12), NONE(13), LONG(14);

        private final int etkNumber;

        /**
         * @param entellitrakNumber
         *            The number which core entellitrak uses to refer to this
         *            type of element in the database
         */
        private DataElementType(final int myEtkNumber) {
            etkNumber = myEtkNumber;
        }

        /**
         * @return The number that core entellitrak uses to refer to this data
         *         element
         */
        public int getEtkNumber() {
            return etkNumber;
        }

        public String getEtkNumberAsString() {
            return String.valueOf(etkNumber);
        }

        /**
         * This method converts the core entellitrak number for a data element
         * type into an enum.
         *
         * @param entellitrakNumber
         *            A number which entellitrak uses to identify a data element
         *            type.
         * @return {@link DataElementType} representing the given entellitrak
         *         id.
         */
        public static DataElementType getDataElementType(final long etkNumber) {

            for (final DataElementType type : DataElementType.values()) {
                if (type.getEtkNumber() == etkNumber) {
                    return type;
                }
            }

            throw new IllegalArgumentException(String.format("\"%s\" is not a number used by core entellitrak to represent a data type.", etkNumber));
        }
    }

    // ETK Data Object...
    public enum DataObjectType {

        // ETK data object types...
        // NONE doesn't seem to be used for anything.
        NONE(0),

        // Tracked Data Object (BTO/CTO)
        TRACKING(1),

        // Reference Data Object (RDO)
        REFERENCE(2),

        // EScan Data Object
        ESCAN(3);

        private final int etkNumber;

        /**
         * @param entellitrakNumber
         *            The number which core entellitrak uses to refer to this
         *            type of object in the database
         */
        private DataObjectType(final int myEtkNumber) {
            etkNumber = myEtkNumber;
        }

        /**
         * @return The number that core entellitrak uses to refer to this data
         *         object type.
         */
        public int getEtkNumber() {
            return etkNumber;
        }

        /**
         * This method converts the core entellitrak number for a data object
         * type into an enum.
         *
         * @param entellitrakNumber
         *            A number which entellitrak uses to identify a data object
         *            type.
         * @return {@link DataObjectType} representing the given entellitrak id.
         */
        public static DataObjectType getDataObjectType(final int etkNumber) {

            for (final DataObjectType type : DataObjectType.values()) {
                if (type.getEtkNumber() == etkNumber) {
                    return type;
                }
            }

            throw new IllegalArgumentException(String.format("\"%s\" is not a number used by core entellitrak to represent a data object type.", etkNumber));
        }
    }

    // Encapsulate ETK Data Object properties...
    public static class DataObjectInfo implements Serializable {
        private static final long serialVersionUID = -2523484931584332745L;

        private String businessKey;
        private Boolean isBaseObject;
        private String label;
        private String name;
        private String objectName;
        private DataObjectType dataObjectType;
        private String tableName;
        private Long dataObjectId;
        private Long parentDataObjectId;
        private String parentBusinessKey;
        private String parentTableName;
        private Map<String, DataElementInfo> mpDataElementsByBusinessKey;
        private Map<String, DataElementInfo> mpDataElementsByColumn;
        private Map<String, DataElementInfo> mpDataElementsByEleName;
        private Map<String, Map<String, Object>> mpChildObjects;

        public String getBusinessKey() {
            return businessKey;
        }

        // Get a list of (immediate) children by business key and a map of other props
        // Map keys: data_object_id, table_name, business_key, name, label
        public Map<String, Map<String, Object>> getChildObjects(ExecutionContext etk) {
            Map<String, Map<String, Object>> retval = mpChildObjects;

            try {
                // Need to get from db?
                if (retval == null) {
                    retval = new HashMap<String, Map<String, Object>>();

                    List<Map<String, Object>> lstObjects = etk.createSQL("select data_object_id, table_name, business_key, name, label"
                        + " from etk_data_object "
                        + " where (parent_object_id = :obj_id)")
                        .setParameter("obj_id", getDataObjectId())
                        .returnEmptyResultSetAs(new ArrayList<Map<String, Object>>())
                        .fetchList();

                    // build result...
                    for (Map<String, Object> mpObjInfo : lstObjects) {
                        retval.put(MapHelper.getMapValueAsString(mpObjInfo, "business_key"), mpObjInfo);
                    }

                    // cache result...
                    setChildObjects(retval);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            return retval;
        }

        // Default interface is by Element Business Key...
        public DataElementInfo getDataElementInfo(String eleBusinessKey) {
            return getDataElementInfo(null, null, eleBusinessKey);
        }

        public DataElementInfo getDataElementInfoByColumnName(String columnName) {
            return getDataElementInfo(null, columnName, null);
        }

        public DataElementInfo getDataElementInfoByName(String eleName) {
            return getDataElementInfo(eleName, null, null);
        }

        // Base method...
        public DataElementInfo getDataElementInfo(String eleName, String columnName, String eleBusinessKey) {
            DataElementInfo retval = null;

            // Find by Name?
            if (StringUtils.isNotBlank(eleName)) {
                retval = (DataElementInfo) MapHelper.getMapValue(mpDataElementsByEleName, eleName.toUpperCase());
            } else if (StringUtils.isNotBlank(columnName)) {
                retval = (DataElementInfo) MapHelper.getMapValue(mpDataElementsByColumn, columnName.toUpperCase());
            } else if (StringUtils.isNotBlank(eleBusinessKey)) {
                retval = (DataElementInfo) MapHelper.getMapValue(mpDataElementsByBusinessKey, eleBusinessKey.toUpperCase());
            }

            return retval;
        }

        // return all data element info object as List()...
        public List<DataElementInfo> getDataElements() {
            List<DataElementInfo> retval = null;

            if (!mpDataElementsByEleName.isEmpty()) {
                retval = new ArrayList<DataElementInfo>(mpDataElementsByEleName.values());
            }

            return retval;
        }

        // return all data element info object as List()...
        public List<DataElementInfo> getDataElementsByBusinessKey() {
            List<DataElementInfo> retval = null;

            if (!mpDataElementsByBusinessKey.isEmpty()) {
                // make sorted map...
                Map<String, DataElementInfo> treeMap = new TreeMap<String, DataElementInfo>(mpDataElementsByBusinessKey);
                retval = new ArrayList<DataElementInfo>(treeMap.values());
            }

            return retval;
        }

        public Long getDataObjectId() {
            return dataObjectId;
        }

        public DataObjectType getDataObjectType() {
            return dataObjectType;
        }

        public Boolean getIsBaseObject() {
            return isBaseObject;
        }

        public String getLabel() {
            return label;
        }

        public String getName() {
            return name;
        }

        public String getObjectName() {
            return objectName;
        }

        public String getParentBusinessKey() {
            return parentBusinessKey;
        }

        public Long getParentDataObjectId() {
            return parentDataObjectId;
        }

        public DataObjectInfo getParentDataObjectInfo(ExecutionContext etk) {
            return getEtkDataObjectInfo(etk, getParentBusinessKey());
        }

        public String getParentTableName() {
            return parentTableName;
        }

        public String getTableName() {
            return tableName;
        }

        // setters...
        public void setBusinessKey(String myBusinessKey) {
            businessKey = myBusinessKey;
        }

        private void setChildObjects(Map<String, Map<String, Object>> myChildObjects) {
            mpChildObjects = myChildObjects;
        }

        public void setDataObjectId(Long myDataObjectId) {
            dataObjectId = myDataObjectId;
        }

        public void setDataElementMapByBusinessKey(Map<String, DataElementInfo> myMap) {
            mpDataElementsByBusinessKey = myMap;
        }

        public void setDataElementMapByColumn(Map<String, DataElementInfo> myMap) {
            mpDataElementsByColumn = myMap;
        }

        public void setDataElementMapByEleName(Map<String, DataElementInfo> myMap) {
            mpDataElementsByEleName = myMap;
        }

        public void setIsBaseObject(Boolean myIsBto) {
            isBaseObject = myIsBto;
        }

        public void setDataObjectType(DataObjectType myDot) {
            dataObjectType = myDot;
        }

        public void setLabel(String myLabel) {
            label = myLabel;
        }

        public void setName(String myName) {
            name = myName;
        }

        public void setObjectName(String myName) {
            objectName = myName;
        }

        public void setParentBusinessKey(String myBusinessKey) {
            parentBusinessKey = myBusinessKey;
        }

        public void setParentDataObjectId(Long myDataObjectId) {
            parentDataObjectId = myDataObjectId;
        }

        public void setParentTableName(String myTableName) {
            parentTableName = myTableName;
        }

        public void setTableName(String myTableName) {
            tableName = myTableName;
        }
    }

    // Encapsulate ETK Data Element properties...
    public static class DataElementInfo implements Serializable {
        private static final long serialVersionUID = 2478774766484336811L;

        private String businessKey;
        private String columnName;
        private Long dataElementId;
        private DataElementType dataType;
        private String elementName;
        private String formLabel;
        private Boolean isIdentifier;
        private Boolean isLookup;
        private Boolean isMultiSelect;
        private Boolean isRequired;
        private Long lookupDefId;
        private String lookupBusinessKey;
        private String lookupTableName;
        private String name;

        // Constructor...
        public DataElementInfo() {
            // defaults...
            setIsLookup(false);
            setIsMultiSelect(false);
        }

        public String getBusinessKey() {
            return businessKey;
        }

        public String getColumnName() {
            return columnName;
        }

        public Long getDataElementId() {
            return dataElementId;
        }

        public DataElementType getDataType() {
            return dataType;
        }

        public String getElementName() {
            return elementName;
        }

        public String getFormLabel() {
            return formLabel;
        }

        public Boolean getIsIdentifier() {
            return isIdentifier;
        }

        public Boolean getIsLookup() {
            return isLookup;
        }

        public Boolean getIsMultiSelect() {
            return isMultiSelect;
        }

        public Boolean getIsRequired() {
            return isRequired;
        }

        public String getLookupBusinessKey() {
            return lookupBusinessKey;
        }

        public Long getLookupDefId() {
            return lookupDefId;
        }

        public String getLookupTableName() {
            return lookupTableName;
        }

        public String getName() {
            return name;
        }

        // setters...
        public void setBusinessKey(String myBk) {
            businessKey = myBk;
        }

        public void setColumnName(String myColumnName) {
            columnName = myColumnName;
        }

        public void setDataElementId(Long myId) {
            dataElementId = myId;
        }

        public void setDataElementType(DataElementType myDet) {
            dataType = myDet;
        }

        public void setElementName(String myElementName) {
            elementName = myElementName;
        }

        public void setFormLabel(String myName) {
            formLabel = myName;
        }

        public void setIsIdentifier(Boolean myIsIdentifier) {
            isIdentifier = myIsIdentifier;
        }

        public void setIsLookup(Boolean myIsLookup) {
            isLookup = myIsLookup;
        }

        public void setIsMultiSelect(Boolean myIsMultiSelect) {
            isMultiSelect = myIsMultiSelect;
        }

        public void setIsRequired(Boolean myIsRequired) {
            isRequired = myIsRequired;
        }

        public void setLookupBusinessKey(String myBusinessKey) {
            lookupBusinessKey = myBusinessKey;
        }

        public void setLookupDefId(Long myId) {
            lookupDefId = myId;
        }

        public void setLookupTableName(String myTableName) {
            lookupTableName = myTableName;
        }

        public void setName(String myName) {
            name = myName;
        }
    }

    // Delete All children of ETK Object (and all dependencies)...
    static public int deleteAllChildrenOfParent(ExecutionContext etk, String parentBusinessKey, Long parentTrackingId) {
        // return the number of deletes
        int retval = 0;

        // Lookup etk object...
        DataObjectInfo myDoi = getEtkDataObjectInfo(etk, parentBusinessKey);
        if (myDoi != null) {
            // return the number of deletes
            retval = deleteChildObject(etk, myDoi.getTableName(), parentTrackingId, false, 0);
        }

        return retval;
    }

    // Delete all ETK Assignments...
    static public int deleteAssignments(ExecutionContext etk, String objBusinessKey) {
        return deleteAssignments(etk, objBusinessKey, null, null, null);
    }

    // Delete all ETK Assignments...
    static public int deleteAssignments(ExecutionContext etk, String objBusinessKey, Long trackingId, Long subjectId, Long roleId) {
        int retval = 0;

        String extraWhere = "";
        if (trackingId != null)
            extraWhere += " and (tracking_id = :trackingId)";
        if (subjectId != null)
            extraWhere += " and (subject_id = :subjectId)";
        if (roleId != null)
            extraWhere += " and (role_id = :roleId)";

        // Object is required, other params optional...
        if (StringUtils.isNotBlank(objBusinessKey)) {
            // Get object props...
            DataObjectInfo myDoi = getEtkDataObjectInfo(etk, objBusinessKey);
            if (myDoi != null) {
                return etk.createSQL("delete from etk_assignment where (data_object_key = :objTableNm)" + extraWhere)
                    .setParameter("objTableNm", myDoi.tableName)
                    .setParameter("trackingId", trackingId)
                    .setParameter("subjectId", subjectId)
                    .setParameter("roleId", roleId)
                    .execute();
            }
        }

        return retval;
    }

    // Delete a single child of ETK Object (and all dependencies)...
    static public int deleteChildrenOfParent(ExecutionContext etk, String childBusinessKey, Long parentTrackingId) {
        // return the number of deletes
        int retval = 0;

        // Lookup etk object...
        DataObjectInfo myDoi = getEtkDataObjectInfo(etk, childBusinessKey);
        if (myDoi != null) {
            // get child objects...
            List<Map<String, Object>> lstRecs = etk.createSQL("select id from " + myDoi.getTableName()
                + " where id_parent = :parentId")
                .returnEmptyResultSetAs(null)
                .setParameter("parentId", parentTrackingId)
                .fetchList();
            if (lstRecs != null) {
                // For each rec, delete...
                for (Map<String, Object> recMap : lstRecs) {
                    retval += deleteChildObject(etk, myDoi.getTableName(), MapHelper.getMapValueAsLong(recMap, "id"), true, 0);
                }
            }
        }

        return retval;
    }

    // Delete ETK Object and all it's children and dependencies...
    static public int deleteObject(ExecutionContext etk, String objBusinessKey, Long trackingId) {
        // return the number of deletes
        int retval = 0;

        // Lookup etk object...
        DataObjectInfo myDoi = getEtkDataObjectInfo(etk, objBusinessKey);
        if (myDoi != null) {
            if (myDoi.isBaseObject) {
                // Call deleteWorkflow on BTO...
                if ((etk.getWorkflowService() != null) && etk.getWorkflowService().deleteWorkflow(objBusinessKey, trackingId))
                    retval = 1;
            } else {
                // Start recursive delete...
                retval = deleteChildObject(etk, myDoi.getTableName(), trackingId, true, 0);
            }
        }

        return retval;
    }

    // recursive...
    static private int deleteChildObject(ExecutionContext etk, String argTableName, Long argTrackingId, boolean doParentDelete, int objLevel) {
        // return the number of deletes
        int retval = 0;

        // Are there children?

        // Get child tables...
        List<Map<String, Object>> lstChildTables = etk.createSQL("SELECT child.table_name"
            + " FROM etk_data_object parent"
            + "   JOIN etk_data_object child ON (child.parent_object_id = parent.data_object_id)"
            + " where (parent.tracking_config_id = (select max(tracking_config_id) from etk_tracking_config_archive))"
            + "   and (parent.table_name = :edoTableName)")
            .returnEmptyResultSetAs(null)
            .setParameter("edoTableName", argTableName)
            .fetchList();
        if (lstChildTables != null) {
            // Get id's of all child recs...
            for (Map<String, Object> myMap : lstChildTables) {
                String childTable = MapHelper.getMapValueNonNull(myMap, "table_name");

                List<Map<String, Object>> lstRecs = etk.createSQL("select id from " + childTable
                    + " where id_parent = :parentId")
                    .returnEmptyResultSetAs(null)
                    .setParameter("parentId", argTrackingId)
                    .fetchList();
                if (lstRecs != null) {
                    // For each rec, recurse...
                    for (Map<String, Object> recMap : lstRecs) {
                        retval += deleteChildObject(etk, childTable, MapHelper.getMapValueAsLong(recMap, "id"), doParentDelete, objLevel + 1);
                    }
                }
            }
        }

        // delete this rec...
        if ((objLevel > 0) || doParentDelete) {
            // delete all M table entries for all fields...
            List<Map<String, Object>> lstLookupTables = etk.createSQL("SELECT ede.table_name"
                + " FROM etk_data_object edo"
                + "   JOIN etk_data_element ede ON (ede.data_object_id = edo.data_object_id)"
                + " where (edo.tracking_config_id = (select max(tracking_config_id) from etk_tracking_config_archive))"
                + "   and (edo.table_name = :edoTableName) AND (ede.table_name IS NOT NULL)")
                .returnEmptyResultSetAs(null)
                .setParameter("edoTableName", argTableName)
                .fetchList();
            if (lstLookupTables != null) {
                for (Map<String, Object> myMap : lstLookupTables) {
                    String lookupTable = MapHelper.getMapValueNonNull(myMap, "table_name");

                    // delete lookup refs...
                    etk.createSQL("DELETE FROM " + lookupTable + " WHERE (id_owner = :trackingId)")
                        .setParameter("trackingId", argTrackingId)
                        .execute();
                }
            }

            // delete all (non-DM) ETK_FILE entries...
            etk.createSQL("DELETE FROM etk_file WHERE (object_type = :tableName) AND (reference_id = :trackingId) AND (etk_dm_resource_id is null)")
                .setParameter("tableName", argTableName)
                .setParameter("trackingId", argTrackingId)
                .execute();

            // finally delete rec...
            // etk.getLogger().error("delete from " + argTableName + " where id = " + argTrackingId);
            etk.createSQL("DELETE FROM " + argTableName + " where (id = :trackingId)")
                .setParameter("trackingId", argTrackingId)
                .execute();

            retval += 1;
        }

        return retval;
    }

    public static ApplicationException getApplicationException(ExecutionContext etk, String msg) {
        return getApplicationException(etk, new ApplicationException(msg));
    }

    public static ApplicationException getApplicationException(ExecutionContext etk, Exception ex) {
        return getApplicationException(etk, ex, "AIM Exception");
    }

    public static ApplicationException getApplicationException(ExecutionContext etk, Exception ex, String msg) {
        ApplicationException retval;
        boolean doLog = false;

        // debug...
        // if ((etk != null) && (ex != null)) {
        // etk.getLogger().error("Exception class: " + ex.getClass().getSimpleName());
        // }

        // Always return AE, log just once...
        if ((ex != null) && ex.getClass().equals(ApplicationException.class)) {
            if (ex.getCause() == null) {
                doLog = true;
                ex.initCause(ex);
            }

            retval = (ApplicationException) ex;
        } else {
            // ETK logs these exceptions (so don't log twice)...
            // if (!java.sql.SQLException.class.isAssignableFrom(ex.getClass()))
            if (!ex.getClass().equals(DataAccessException.class))
                doLog = true;
            retval = new ApplicationException(ex);
        }

        // Log?
        if (doLog) {
            if (etk == null) {
                if (ex != null)
                    ex.printStackTrace();
            } else {
                if (StringUtils.isBlank(msg))
                    etk.getLogger().error(ex);
                else
                    etk.getLogger().error(msg, ex);
            }
        }

        return retval;
    }

    static public DataObjectInfo getEtkDataObjectInfo(ExecutionContext etk, String objBusinessKey) {
        return getEtkDataObjectInfo(etk, objBusinessKey, null);
    }

    static public DataObjectInfo getEtkDataObjectInfoByTableName(ExecutionContext etk, String objTableName) {
        return getEtkDataObjectInfo(etk, null, objTableName);
    }

    // create object from ETK data dictionary...
    static protected DataObjectInfo getEtkDataObjectInfo(ExecutionContext etk, String objBusinessKey, String objTableName) {
        Map<String, Object> mpDataObject = null;

        try {
            String extraClause = null;
            if (StringUtils.isNotBlank(objBusinessKey)) {
                extraClause = " and (etkdo.BUSINESS_KEY = :bk)";
            } else if (StringUtils.isNotBlank(objTableName)) {
                extraClause = " and (etkdo.TABLE_NAME = :tn)";
            }

            if (StringUtils.isNotBlank(extraClause)) {
                mpDataObject = etk.createSQL("select etkdo.*, etkpo.BUSINESS_KEY PARENT_BUSINESS_KEY, etkpo.TABLE_NAME PARENT_TABLE_NAME"
                    + " from etk_data_object etkdo"
                    + "   left join etk_data_object etkpo on (etkdo.parent_object_id = etkpo.data_object_id)"
                    + " where etkdo.tracking_config_id = (select max(tracking_config_id) from etk_tracking_config_archive)"
                    + extraClause)
                    .returnEmptyResultSetAs(null)
                    .setParameter("bk", objBusinessKey)
                    .setParameter("tn", objTableName)
                    .fetchMap();
            }
        } catch (final Exception e) {
            etk.getLogger().error("Could not retrieve ETK_DATA_OBJECT");
        }

        // not found?
        if (mpDataObject == null)
            return null;

        // load object props...
        final DataObjectInfo doi = new DataObjectInfo();
        doi.setBusinessKey(MapHelper.getMapValueAsString(mpDataObject, "BUSINESS_KEY"));
        doi.setDataObjectId(MapHelper.getMapValueAsLong(mpDataObject, "DATA_OBJECT_ID"));
        doi.setIsBaseObject(MapHelper.getMapValueAsBoolean(mpDataObject, "BASE_OBJECT"));
        doi.setDataObjectType(DataObjectType.getDataObjectType(MapHelper.getMapValueAsInteger(mpDataObject, "OBJECT_TYPE")));
        doi.setLabel(MapHelper.getMapValueAsString(mpDataObject, "LABEL"));
        doi.setName(MapHelper.getMapValueAsString(mpDataObject, "NAME"));
        doi.setObjectName(MapHelper.getMapValueAsString(mpDataObject, "OBJECT_NAME"));
        doi.setTableName(MapHelper.getMapValueAsString(mpDataObject, "TABLE_NAME"));
        doi.setParentDataObjectId(MapHelper.getMapValueAsLong(mpDataObject, "PARENT_OBJECT_ID"));
        doi.setParentBusinessKey(MapHelper.getMapValueAsString(mpDataObject, "PARENT_BUSINESS_KEY"));
        doi.setParentTableName(MapHelper.getMapValueAsString(mpDataObject, "PARENT_TABLE_NAME"));

        // get element info...
        getEtkDataElements(etk, doi);

        return doi;
    }

    // Lookup Data Elements for an Object...
    static private void getEtkDataElements(ExecutionContext etk, DataObjectInfo doi) {
        List<Map<String, Object>> etkDataElementInfoList = null;

        if ((etk == null) || (doi.getDataObjectId() == null)) {
            return;
        }

        try {
            etkDataElementInfoList = etk.createSQL("select etkde.*, df.label FORM_LABEL, etkld.business_key lookup_definition_bk"
                + " from etk_data_element etkde"
                + "   left outer join (select etkdf.data_object_id, etkeb.data_element_id, etkfc.*"
                + "     from ETK_FORM_CONTROL etkfc"
                + "       join ETK_FORM_CTL_ELEMENT_BINDING etkeb on (etkfc.form_control_id = etkeb.form_control_id)"
                + "       join ETK_DATA_FORM etkdf on (etkdf.data_form_id = etkfc.data_form_id)"
                + "     where (etkdf.default_form = 1)) df on (etkde.data_object_id = df.data_object_id) and (etkde.data_element_id = df.data_element_id)"
                + "   left outer join ETK_LOOKUP_DEFINITION etkld on (etkde.lookup_definition_id = etkld.lookup_definition_id)"
                + " where (etkde.DATA_OBJECT_ID = :dataObjectId) order by etkde.BUSINESS_KEY")
                .returnEmptyResultSetAs(new ArrayList<Map<String, Object>>())
                .setParameter("dataObjectId", doi.getDataObjectId())
                .fetchList();
        } catch (final Exception e) {
            etk.getLogger().error("Could not retrieve ETK_DATA_ELEMENT rows for DATA_OBJECT_ID = " + doi.getDataObjectId());
            return;
        }

        // Create index maps...
        Map<String, DataElementInfo> mpDataElementsByName = new HashMap<String, DataElementInfo>();
        Map<String, DataElementInfo> mpDataElementsByColumn = new HashMap<String, DataElementInfo>();
        Map<String, DataElementInfo> mpDataElementsByBusinessKey = new HashMap<String, DataElementInfo>();

        // iterate Elements...
        for (Map<String, Object> mpDataElement : etkDataElementInfoList) {
            // Load DataElementInfo...
            final DataElementInfo myDei = new DataElementInfo();
            myDei.setDataElementId(MapHelper.getMapValueAsLong(mpDataElement, "DATA_ELEMENT_ID"));
            myDei.setBusinessKey(MapHelper.getMapValueAsString(mpDataElement, "BUSINESS_KEY"));
            myDei.setIsRequired(MapHelper.getMapValueAsBoolean(mpDataElement, "REQUIRED"));
            myDei.setDataElementType(DataElementType.getDataElementType(MapHelper.getMapValueAsInteger(mpDataElement, "DATA_TYPE")));
            myDei.setName(MapHelper.getMapValueAsString(mpDataElement, "NAME"));
            myDei.setElementName(MapHelper.getMapValueAsString(mpDataElement, "ELEMENT_NAME"));
            myDei.setColumnName(MapHelper.getMapValueAsString(mpDataElement, "COLUMN_NAME"));
            myDei.setFormLabel(MapHelper.getMapValueAsString(mpDataElement, "FORM_LABEL"));
            myDei.setLookupTableName(MapHelper.getMapValueAsString(mpDataElement, "TABLE_NAME"));
            myDei.setLookupDefId(MapHelper.getMapValueAsLong(mpDataElement, "LOOKUP_DEFINITION_ID"));
            myDei.setIsIdentifier(MapHelper.getMapValueAsBoolean(mpDataElement, "IDENTIFIER"));
            if (myDei.getLookupDefId() != null) {
                myDei.setIsLookup(true);
                myDei.setLookupBusinessKey(MapHelper.getMapValueAsString(mpDataElement, "LOOKUP_DEFINITION_BK"));
            }
            if (StringUtils.isNotBlank(myDei.getLookupTableName())) {
                myDei.setIsLookup(true);
                myDei.setIsMultiSelect(true);
            }

            // Add to indexes...
            mpDataElementsByName.put(myDei.getElementName().toUpperCase(), myDei);
            mpDataElementsByColumn.put(myDei.getColumnName().toUpperCase(), myDei);
            mpDataElementsByBusinessKey.put(myDei.getBusinessKey().toUpperCase(), myDei);
        }

        // add to data object...
        doi.setDataElementMapByEleName(mpDataElementsByName);
        doi.setDataElementMapByColumn(mpDataElementsByColumn);
        doi.setDataElementMapByBusinessKey(mpDataElementsByBusinessKey);

        // etk.getLogger().error("EtkHelper debug:\n" + mpDataElementsByName + "\n" + mpDataElementsByColumn + "\n" + mpDataElementsByBusinessKey + "\n");
    }

    static public Long getCurrentUserId(ExecutionContext etk) {
        return (etk.getCurrentUser() == null ? ETK_ADMINUSER_ID : etk.getCurrentUser().getId());
    }

    static public String getCurrentUserLogin(ExecutionContext etk) {
        return (etk.getCurrentUser() == null ? ETK_ADMINUSER_LOGIN : etk.getCurrentUser().getAccountName());
    }

    static public String getCurrentUserRole(ExecutionContext etk) {
        return (etk.getCurrentUser() == null ? ETK_ADMINUSER_ROLE_BK
            : (etk.getCurrentUser().getRole() == null ? ETK_ADMINUSER_ROLE_BK
                : etk.getCurrentUser().getRole().getBusinessKey()));
    }

    static public Long getCurrentUserRoleId(ExecutionContext etk) {
        return (etk.getCurrentUser() == null ? null
            : (etk.getCurrentUser().getRole() == null ? null
                : etk.getCurrentUser().getRole().getId()));
    }

    static public String getCurrentUserRoleName(ExecutionContext etk) {
        return (etk.getCurrentUser() == null ? ETK_ADMINUSER_ROLE_NAME
            : (etk.getCurrentUser().getRole() == null ? ETK_ADMINUSER_ROLE_NAME
                : etk.getCurrentUser().getRole().getName()));
    }

    // Generic get single db value (as Long)
    static public Long getDatabaseValueLong(ExecutionContext etk, String tblName, String colNameToReturn, String whereClause) {
        Long retval = null;

        String strVal = getDatabaseValueString(etk, tblName, colNameToReturn, whereClause);
        if (StringUtils.isNumeric(strVal))
            retval = Long.parseLong(strVal);

        return retval;
    }

    // Generic get single db value (as String)
    static public String getDatabaseValueString(ExecutionContext etk, String tblName, String colNameToReturn, String whereClause) {
        String retval = null;

        try {
            if ((etk != null)
                && StringUtils.isNotBlank(tblName)
                && StringUtils.isNotBlank(colNameToReturn)
                && StringUtils.isNotBlank(whereClause)) {
                retval = etk.createSQL("select " + colNameToReturn + " from " + tblName
                    + " " + whereClause)
                    .returnEmptyResultSetAs(null)
                    .fetchString();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return retval;
    }

    // Return c_code value for given lookup...
    static public String getDatabaseValueString(ExecutionContext etk, String tblName, String colNameToReturn, Number trackingId) {
        String retval = null;

        if (trackingId != null) {
            retval = getDatabaseValueString(etk, tblName, colNameToReturn, "where id = " + trackingId);
        }

        return retval;
    }

    // Get ETK Role Id from Code...
    static public Long getRoleIdFromCode(ExecutionContext etk, String roleCode) {
        Long retval = null;

        // Read from DB...
        try {
            String strVal = etk.createSQL("select role_id from etk_role where (business_key = :roleCd)")
                .setParameter("roleCd", roleCode)
                .fetchString();

            if (StringUtils.isNumeric(strVal))
                retval = Long.parseLong(strVal);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return retval;
    }

    // Generic Log...
    static public void log(ExecutionContext etk, String msg) {
        etk.getLogger().error(msg);
    }

    // Log Exception...
    static public void log(ExecutionContext etk, Exception ex) {
        log(etk, ex, null);
    }

    // Log Exception...
    static public void log(ExecutionContext etk, Exception ex, String optMsg) {
        if (StringUtils.isBlank(optMsg))
            etk.getLogger().error(ex);
        else
            etk.getLogger().error(optMsg, ex);
    }

}
