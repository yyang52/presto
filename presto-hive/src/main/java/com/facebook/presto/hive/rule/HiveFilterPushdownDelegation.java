/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.rule;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class HiveFilterPushdownDelegation
{
    private HiveFilterPushdownDelegation() {}
    public static String toJson(ProjectNode projectNode,
                                HiveTableLayoutHandle hiveTableLayoutHandle)
    {
        FilterNode filterNode = (FilterNode) projectNode.getSource();
        // construct tableScan JSON node
        TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();
        // all columns <name, hiveType>
        List<Column> dataColumns = hiveTableLayoutHandle.getDataColumns();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode tableScanJsonNode = objectMapper.createObjectNode();
        tableScanJsonNode.put("id", tableScanNode.getId().toString());
        tableScanJsonNode.put("relOp", "LogicalTableScan");
        ArrayNode fieldsNode = objectMapper.createArrayNode();
        for (Column column : dataColumns) {
            fieldsNode.add(column.getName());
        }
        tableScanJsonNode.set("fieldNames", fieldsNode);
        ArrayNode tableNode = objectMapper.createArrayNode();
        String schemaName = hiveTableLayoutHandle.getSchemaTableName().getSchemaName();
        String tableName = hiveTableLayoutHandle.getSchemaTableName().getTableName();
        tableNode.add(schemaName);
        tableNode.add(tableName);
        tableScanJsonNode.set("table", tableNode);
        ArrayNode inputNode = objectMapper.createArrayNode();
        tableScanJsonNode.set("inputs", inputNode);
        ArrayNode relsNode = objectMapper.createArrayNode();
        relsNode.add(tableScanJsonNode);
        relsNode.add(getJsonFilterNode(filterNode));
        relsNode.add(getJsonProjectNode(projectNode));
        ObjectNode rootNode = objectMapper.createObjectNode();
        rootNode.set("rels", relsNode);
        return rootNode.toString();
    }

    private static JsonNode getJsonFilterNode(FilterNode filterNode)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("id", filterNode.getId().toString());
        objectNode.put("relOp", "LogicalFilter");
        ObjectNode conditionNode = objectMapper.createObjectNode();
        if (filterNode.getPredicate() instanceof CallExpression) {
            CallExpression expression = (CallExpression) filterNode.getPredicate();
            conditionNode.put("op", matchOperator(expression.getDisplayName()));
            ArrayNode operandsNode = objectMapper.createArrayNode();
            ObjectNode operandsFeilds = objectMapper.createObjectNode();
            List<RowExpression> arguments = expression.getArguments();
            for (RowExpression rowExpression : arguments) {
                if (rowExpression instanceof ConstantExpression) {
                    int literal =
                            Integer.parseInt(((ConstantExpression) rowExpression).getValue().toString());
                    operandsFeilds.put("literal", literal);
                }
                if (rowExpression instanceof VariableReferenceExpression) {
                    // get columnIndex, hiveType, typeName from TableScanNode.assignments
                    HiveColumnHandle hiveColumnHandle = getColumnHandle(
                            (VariableReferenceExpression) rowExpression,
                            (TableScanNode) filterNode.getSource());
                    int columnIndex = hiveColumnHandle.getHiveColumnIndex();
                    ObjectNode operandsInput = objectMapper.createObjectNode();
                    // column index
                    operandsInput.put("input", columnIndex);
                    operandsNode.add(operandsInput);
                    String type = hiveColumnHandle.getHiveType().toString();
                    String targetType = hiveColumnHandle.getTypeSignature().toString();
                    operandsFeilds.put("type", matchType(type));
                    operandsFeilds.put("target_type", matchType(targetType));
                    operandsFeilds.put("scale", "0");
                    operandsFeilds.put("precision", "10");
                    operandsFeilds.put("type_scale", "0");
                    operandsFeilds.put("type_precision", "10");
                    operandsNode.add(operandsFeilds);
                }
            }
            ObjectNode typeFieldsNode = objectMapper.createObjectNode();
            typeFieldsNode.put("type", expression.getType().toString());
            typeFieldsNode.put("nullable", true);
            conditionNode.set("operands", operandsNode);
            conditionNode.set("type", typeFieldsNode);
        }
        objectNode.set("condition", conditionNode);
        return objectNode;
    }

    private static String matchOperator(String originOperator)
    {
        switch (originOperator) {
            case "EQUAL": return "=";
        }
        return originOperator;
    }

    private static String matchType(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "Int":
                return "Integer";
        }
        return type;
    }

    private static JsonNode getJsonProjectNode(ProjectNode projectNode)
    {
        TableScanNode tableScanNode = (TableScanNode) ((FilterNode) projectNode.getSource()).getSource();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("id", projectNode.getId().toString());
        objectNode.put("relOp", "LogicalProject");
        Set<VariableReferenceExpression> assignments = projectNode.getAssignments().getVariables();
        ArrayNode exprsFeilds = objectMapper.createArrayNode();
        for (VariableReferenceExpression expression : assignments) {
            HiveColumnHandle hiveColumnHandle = getColumnHandle(expression, tableScanNode);
            ObjectNode exprsFeild = objectMapper.createObjectNode();
            exprsFeild.put("literal", hiveColumnHandle.getHiveColumnIndex());
            exprsFeild.put("type", hiveColumnHandle.getHiveType().toString());
            exprsFeild.put("target_type", hiveColumnHandle.getTypeSignature().toString());
            exprsFeilds.add(exprsFeild);
        }
        objectNode.set("exprs", exprsFeilds);
        return objectNode;
    }

    public static String toJson(HiveTableLayoutHandle hiveTableLayoutHandle)
    {
        List<Column> dataColumns = hiveTableLayoutHandle.getDataColumns();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        ArrayNode columns = objectMapper.createArrayNode();
        for (Column column : dataColumns) {
            ObjectNode singleColumn = objectMapper.createObjectNode();
            singleColumn.put(column.getName(), column.getType().toString().toUpperCase());
            columns.add(singleColumn);
        }
        objectNode.set("columns", columns);
        return objectNode.toString();
    }

    private static HiveColumnHandle getColumnHandle(VariableReferenceExpression expression,
                                                    TableScanNode tableScan)
    {
        Map<VariableReferenceExpression, ColumnHandle> assignments = tableScan.getAssignments();
        return (HiveColumnHandle) assignments.get(expression);
    }
}
