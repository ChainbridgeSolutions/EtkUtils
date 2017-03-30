/**
 *
 * Wraps / abstracts various ETK execution context objects.
 *
 * psmiley 10/05/2016
 **/

package gov.atf.bi.common.helper;


import org.apache.commons.lang.StringUtils;

import com.entellitrak.ApplicationException;
import com.entellitrak.ExecutionContext;
import com.entellitrak.DataObjectEventContext;
import com.entellitrak.DataEventType;
import com.entellitrak.WorkflowExecutionContext;
import com.entellitrak.configuration.DataObject;
import com.entellitrak.dynamic.DataObjectInstance;
import com.entellitrak.legacy.workflow.AssignmentInfo;
import com.entellitrak.legacy.workflow.WorkflowResult;
import com.entellitrak.FormInfo;
import com.entellitrak.SQLFacade;


//Abstraction of ETK Contexts...
public class EtkContextHelper {
    private DataObjectEventContext doeContext = null;
    private WorkflowExecutionContext weContext = null;

    public ExecutionContext etkContext = null;
    public boolean isBaseObject = false;
    public boolean isCreateAssignmentEvent = false;
    public boolean isCreateEvent = false;
    public boolean isCreateOrUpdateEvent = false;
    public boolean isCreateOrUpdateAssignmentEvent = false;
    public boolean isDataObjectEventContext = false;
    public boolean isDeleteEvent = false;
    public boolean isNewEvent = false;
    public boolean isReadEvent = false;
    public boolean isUpdateAssignmentEvent = false;
    public boolean isUpdateEvent = false;
    public boolean isWorkflowExecutionContext = false;
    public DataObjectInstance newObject = null;
    public String objBusinessKey = null;
    public String baseObjBusinessKey = null;
    public String parentObjBusinessKey = null;
    public DataObjectInstance oldObject = null;
    public Long baseId = null;
    public Long parentId = null;
    public Long trackingId = null;
    public WorkflowResult wkfResult = null;

    // DataObjectEventContext constructor...
    public EtkContextHelper(DataObjectEventContext etk) throws ApplicationException {
        // init vars...
        isDataObjectEventContext = true;
        doeContext = etk;
        etkContext = doeContext;
        wkfResult = doeContext.getResult();
        newObject = doeContext.getNewObject();
        oldObject = doeContext.getOldObject();
        isCreateAssignmentEvent = (doeContext.getDataEventType() == DataEventType.ASSIGNMENT_CREATE);
        isCreateEvent = (doeContext.getDataEventType() == DataEventType.CREATE);
        isDeleteEvent = (doeContext.getDataEventType() == DataEventType.DELETE);
        isNewEvent = (doeContext.getDataEventType() == DataEventType.NEW);
        isReadEvent = (doeContext.getDataEventType() == DataEventType.READ);
        isUpdateAssignmentEvent = (doeContext.getDataEventType() == DataEventType.ASSIGNMENT_UPDATE);
        isUpdateEvent = (doeContext.getDataEventType() == DataEventType.UPDATE);
        isCreateOrUpdateEvent = (isCreateEvent || isUpdateEvent);
        isCreateOrUpdateAssignmentEvent = (isCreateAssignmentEvent || isUpdateAssignmentEvent);
        baseId = newObject.properties().getBaseId();
        parentId = newObject.properties().getParentId();
        trackingId = newObject.properties().getId();
        objBusinessKey = newObject.configuration().getBusinessKey();

        setObjectConfig(newObject);
    }

    // WorkflowExecutionContext Constructor
    public EtkContextHelper(WorkflowExecutionContext etk) throws ApplicationException {
        isWorkflowExecutionContext = true;
        weContext = etk;
        etkContext = etk;
        wkfResult = etk.getResult();
        newObject = etk.getNewObject();
        oldObject = etk.getOldObject();
        isCreateAssignmentEvent = etk.isCreateAssignmentEvent();
        isCreateEvent = etk.isCreateEvent();
        isDeleteEvent = etk.isDeleteEvent();
        isReadEvent = etk.isReadEvent();
        isUpdateAssignmentEvent = etk.isUpdateAssignmentEvent();
        isUpdateEvent = etk.isUpdateEvent();
        isCreateOrUpdateEvent = (isCreateEvent || isUpdateEvent);
        isCreateOrUpdateAssignmentEvent = (isCreateAssignmentEvent || isUpdateAssignmentEvent);
        baseId = newObject.properties().getBaseId();
        parentId = newObject.properties().getParentId();
        trackingId = newObject.properties().getId();
        objBusinessKey = newObject.configuration().getBusinessKey();

        setObjectConfig(newObject);
    }

    // Workflow message...
    public void addMessage(String wkfMessage) {
        wkfResult.addMessage(wkfMessage);
    }

    // Cancel...
    public void cancelTransaction(String wkfMessage) throws ApplicationException {
        try {
            addMessage(wkfMessage);
        } catch (Exception ex) {
            throw new ApplicationException(ex.getMessage(), ex);
        }
        this.cancelTransaction();
    }

    public void cancelTransaction() {
        wkfResult.cancelTransaction();
    }

    // shutdown...
    public void close() {
    }

    public SQLFacade createSQL(String sql) {
        return etkContext.createSQL(sql);
    }

    public AssignmentInfo getAssignment() {
        AssignmentInfo retval = null;

        if (isDataObjectEventContext)
            retval = doeContext.getAssignment();
        else if (isWorkflowExecutionContext)
            retval = weContext.getAssignment();

        return retval;
    }

    // Current ETK user...
    public Long getCurrentUserId() {
        return EtkHelper.getCurrentUserId(etkContext);
    }

    // Current ETK user role (bk)...
    public String getCurrentUserRole() {
        return EtkHelper.getCurrentUserRole(etkContext);
    }

    // Get DOE Context (if applicable)
    public DataObjectEventContext getDataObjectEventContext() {
        if (isDataObjectEventContext)
            return doeContext;
        else
            return null;
    }

    // object form...
    public FormInfo getForm() {
        if (isDataObjectEventContext)
            return doeContext.getForm();
        else if (isWorkflowExecutionContext)
            return weContext.getForm();
        return null;
    }

    // object tablename...
    public String getTableName() {
        return newObject.configuration().getTableName();
    }

    // Get Workflow Context (if applicable)
    public WorkflowExecutionContext getWorkflowExecutionContext() {
        if (isWorkflowExecutionContext)
            return weContext;
        else
            return null;
    }

    public boolean isCanceled() {
        return (wkfResult.isTransactionCanceled());
    }

    // simple logger...
    public void logError(String msg) {
        etkContext.getLogger().error(msg);
    }

    public void redirectToAssignments() {
        if (isWorkflowExecutionContext) {
            weContext.redirectToAssignments();
        } else if (isDataObjectEventContext) {
            doeContext.getRedirectManager().redirectToAssignments();
        }
    }

    public void redirectToEdit(String businessKey, Long trackingId) {
        if (isWorkflowExecutionContext) {
            weContext.redirectToEdit(businessKey, trackingId);
        } else if (isDataObjectEventContext) {
            doeContext.getRedirectManager().redirectToEdit(businessKey, trackingId);
        }
    }

    public void redirectToInbox() {
        if (isWorkflowExecutionContext) {
            weContext.redirectToInbox();
        } else if (isDataObjectEventContext) {
            doeContext.getRedirectManager().redirectToInbox();
        }
    }

    public void redirectToList(String businessKey, Long parentId) {
        if (isWorkflowExecutionContext) {
            weContext.redirectToList(businessKey, parentId);
        } else if (isDataObjectEventContext) {
            doeContext.getRedirectManager().redirectToList(businessKey, parentId);
        }
    }

    public void redirectToNew(String businessKey, Long parentId) {
        if (isWorkflowExecutionContext) {
            weContext.redirectToNew(businessKey, parentId);
        } else if (isDataObjectEventContext) {
            doeContext.getRedirectManager().redirectToNew(businessKey, parentId);
        }
    }

    // Use ETK api to discover parent and base object info...
    private void setObjectConfig(DataObjectInstance etkObject) {

        // Get Parent (will throw null ptr if BTO, only way to know)...
        try {
            parentObjBusinessKey = etkObject.configuration().getParent().getBusinessKey();
        } catch (Exception ex) {
        }

        // If no Parent, we're BTO, else find BTO...
        if (StringUtils.isBlank(parentObjBusinessKey)) {
            // Assume BTO...
            baseObjBusinessKey = objBusinessKey;
            isBaseObject = true;
            baseId = trackingId;
            parentId = trackingId;
        } else {
            // Find bto...
            DataObject parentObj = etkObject.configuration().getParent();
            try {
                while ((parentObj != null)) {
                    baseObjBusinessKey = parentObj.getBusinessKey();
                    parentObj = parentObj.getParent();
                }
            } catch (Exception ex) {
            }
        }
    }

    // Custom prop print...
    public String toString() {
        return "\nobjBk=" + objBusinessKey + "\nparentBk=" + parentObjBusinessKey + "\nbaseBk=" + baseObjBusinessKey
            + "\nbaseId=" + baseId + "\nparentId=" + parentId + "\ntrackingId=" + trackingId + "\ntableName="
            + getTableName() + "\n" + (isBaseObject ? "BTO" : "CTO");
    }
}
