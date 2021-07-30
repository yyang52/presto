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
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A builder utils to bridge Presto Plan and Cider Plan (e.g., Json RA)
 */
public class PrestodbPlanBuilder
{
    private PrestodbPlanBuilder() {}

    private static CiderOperatorNode buildCiderNode(ProjectNode node)
    {
        //TODO
        throw new RuntimeException("Unsupported operator");
    }

    // TODO vMap should be passing-in in execution?
    private static CiderOperatorNode buildCiderNode(FilterNode node, Map<VariableReferenceExpression, ColumnHandle> vMap)
    {
        List<CiderExpression> ciderExpressionList = new ArrayList<>();
        if (node.getPredicate() instanceof CallExpression) {
            CallExpression callExpress = (CallExpression) node.getPredicate();
            List<RowExpression> celist = callExpress.getArguments();
            for (RowExpression r : celist) {
                if (r instanceof ConstantExpression) {
                    ConstantExpression ce = (ConstantExpression) r;
                    ciderExpressionList.add(new CiderConstantExpression(r.getType().getDisplayName(), ce.toString()));
                }
                else if (r instanceof VariableReferenceExpression) {
                    VariableReferenceExpression v = (VariableReferenceExpression) r;
                    if (vMap == null) {
                        // vMap should be single time created per table source
                        vMap = getColumnHandle(node);
                    }
                    ColumnHandle ch = vMap.get(v);
                    int columnInx = (ch instanceof HiveColumnHandle) ? ((HiveColumnHandle) ch).getHiveColumnIndex() : 0;
                    ciderExpressionList.add(new CiderVariableReferenceExpression(columnInx, v.getType().getDisplayName()));
                }
                else {
                    //FIXME
                    throw new RuntimeException("Unsupported operator");
                }
            }
            CiderFilterCondition filterCondition = new CiderFilterCondition(
                    callExpress.getDisplayName(), callExpress.getType().toString(), ciderExpressionList);
            CiderOperatorNode n = new CiderFilterNode(filterCondition);
            n.registerChildren(toRAJsonStrHelper(node.getSource(), vMap));
            return n;
        }
        //FIXME need to split plan if we couldn't support
        throw new RuntimeException("Unsupported operator");
    }

    private static CiderOperatorNode buildCiderNode(TableScanNode node, Map<VariableReferenceExpression, ColumnHandle> vMap)
    {
        if (vMap == null) {
            // vMap should be single time created per table source
            vMap = getColumnHandle(node);
        }
        if (node.getTable().getConnectorHandle() instanceof HiveTableHandle) {
            HiveTableHandle hth = (HiveTableHandle) node.getTable().getConnectorHandle();
            List<String> fieldsName = new ArrayList<>();
            for (VariableReferenceExpression r : vMap.keySet()) {
                fieldsName.add(r.getName());
            }
            return new CiderTableScanNode(hth.getSchemaName(), hth.getTableName(), fieldsName);
        }
        //FIXME need to split plan if we couldn't support
        throw new RuntimeException("Unsupported operator");
    }

    private static CiderOperatorNode toRAJsonStrHelper(PlanNode planNode, Map<VariableReferenceExpression, ColumnHandle> vMap)
    {
        if (planNode instanceof ProjectNode) {
            return buildCiderNode((ProjectNode) planNode);
        }
        else if (planNode instanceof FilterNode) {
            return buildCiderNode((FilterNode) planNode, vMap);
        }
        else if (planNode instanceof TableScanNode) {
            return buildCiderNode((TableScanNode) planNode, vMap);
        }
        //FIXME
        throw new RuntimeException("Unsupported operator");
    }

    private static Map<VariableReferenceExpression, ColumnHandle> getColumnHandle(
            PlanNode node)
    {
        return getSourceNode(node).getAssignments();
    }

    private static TableScanNode getSourceNode(
            PlanNode node)
    {
        List<PlanNode> p = node.getSources();
        // Proceed to source node
        while (p.size() == 1) {
            node = node.getSources().get(0);
            p = node.getSources();
        }
        if (p.size() != 0 || !(node instanceof TableScanNode)) {
            //FIXME
            throw new RuntimeException("Unable to resolve variable since multi-source found!");
        }
        return ((TableScanNode) node);
    }

    public static String toSchemaJson(PlanNode planNode)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode rootNode = objectMapper.createObjectNode();
        ArrayNode columns = objectMapper.createArrayNode();
        TableScanNode source = getSourceNode(planNode);
        for (VariableReferenceExpression r : source.getAssignments().keySet()) {
            columns.add(r.getName());
        }
        rootNode.put("Table", ((HiveTableHandle) source.getTable().getConnectorHandle()).getTableName());
        rootNode.set("Columns", columns);
        return rootNode.toString();
    }

    // DFS visitor, TODO unsupporrted plan should return as fallback
    public static String toRAJsonStr(PlanNode planNode)
    {
        return toRAJsonStrHelper(planNode, null).toRAJson();
    }
}
