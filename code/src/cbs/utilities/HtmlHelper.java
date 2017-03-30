/**
 *
 * Html parsing, generation methods
 *
 * psmiley 12/07/2016
 **/

package gov.atf.bi.common.helper;


import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.commons.lang.StringEscapeUtils;

import com.entellitrak.ExecutionContext;


@SuppressWarnings({"rawtypes","unchecked"})
public class HtmlHelper {

    // Generate HTML <table> from query result....
    public static String genHtmlTable(List<Map<String, Object>> lstData, LinkedHashMap<String, String> mpColumns, Map mpParams) {

        StringBuilder output = new StringBuilder();
        output.append("<table"
            + (MapHelper.isValueSet(mpParams, "tableWidth") ? " width='" + MapHelper.getMapValueNonNull(mpParams, "tableWidth") + "'" : "")
            + " name='" + MapHelper.getMapValueNonNull(mpParams, "tableName")
            + "' id='" + MapHelper.getMapValueNonNull(mpParams, "tableId")
            + "' class='" + MapHelper.getMapValueNonNull(mpParams, "tableClass")
            + "'><thead><tr>");

        // Iterate through the headers and create column names
        for (String colHeader : mpColumns.keySet()) {
            if (!"Id".equals(colHeader)) {
                output.append("<th>").append(colHeader).append("</th>");
            }
        }
        output.append("</thead></tr><tbody>");

        // Iterate through the data and populate a grid
        for (Map<String, Object> mpData : lstData) {
            String trHtml = "<tr onclick=\"javascript:showETKModel('" + StringEscapeUtils.escapeHtml(MapHelper.getMapValueNonNull(mpParams, "dataObjectKey")) + "','" 
                + StringEscapeUtils.escapeHtml(MapHelper.getMapValueNonNull(mpData, MapHelper.getMapValue(mpColumns, "Id"))) +   "',900,500,'code.noETKReload','');\">";
            //output.append("<tr>");
            output.append(trHtml);
            
            for (String colHeader : mpColumns.keySet()) {
                if (!"Id".equals(colHeader)) {
                    output.append("<td>")
                        .append(StringEscapeUtils.escapeHtml(MapHelper.getMapValueNonNull(mpData, MapHelper.getMapValue(mpColumns, colHeader))))
                        .append("</td>");
                }
            }
            output.append("</tr>");
        }
        output.append("</tbody></table>");

        return output.toString();
    }

    // Internal test method...
    public static void testHtmlTable(ExecutionContext etk) {
        // Any query, remember to convert Dates to string/char...
        List lstData = etk.createSQL("select * from T_RMI").fetchList();

        // Ordered Map of the table header value (ie. "First Name") and corresponding column name (ie. "C_FIRST_NAME").
        // Only create entries for the columns you wish to see and in the order you see it
        LinkedHashMap<String, String> mpCols = new LinkedHashMap<String, String>();
        mpCols.put("IdHeader", "ID");
        mpCols.put("FIRST_NAME", "C_LEGAL_FIRST_NAME");
        mpCols.put("Last Name", "C_LEGAL_LAST_NAME");
        mpCols.put("SSN", "C_SSN");

        // OPTIONAL params (you may use null for the mpParams argument)
        Map mpParams = new HashMap();
        mpParams.put("tableClass", "testClass");
        mpParams.put("tableId", "testId");
        mpParams.put("tableName", "testName");

        String result = HtmlHelper.genHtmlTable(lstData, mpCols, mpParams);

        etk.getLogger().error("HtmlHelper debug: " + result);
    }
}
