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
package com.facebook.presto.spi.cider;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.mapd.plan.CiderConstantExpression;
import com.mapd.plan.CiderExpression;
import com.mapd.plan.CiderFilterCondition;
import com.mapd.plan.CiderFilterNode;
import com.mapd.plan.CiderOperatorNode;
import com.mapd.plan.CiderTableScanNode;
import com.mapd.plan.CiderVariableReferenceExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A builder utils to bridge Presto Plan and Cider Plan (e.g., Json RA)
 */
public class PrestodbPlanBuilder
{
    private PrestodbPlanBuilder() {}

    private static CiderOperatorNode buildCiderNode(ProjectNode node,
            ColumnAndTableResolver columnAndTableResolver)
    {
        //TODO
        throw new RuntimeException("Unsupported operator");
    }

    private static CiderOperatorNode buildCiderNode(FilterNode node,
            ColumnAndTableResolver ctr)
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
                    int columnInx = ctr.getColumnIndexByColumnHandle(v);
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
            n.registerChildren(toRAJsonStrHelper(node.getSource(), ctr));
            return n;
        }
        //FIXME need to split plan if we couldn't support
        throw new RuntimeException("Unsupported operator");
    }

    private static CiderOperatorNode buildCiderNode(ColumnAndTableResolver ctr)
    {
        return new CiderTableScanNode(ctr.getSchema(), ctr.getTableName(), ctr.getFields());
    }

    private static CiderOperatorNode toRAJsonStrHelper(PlanNode planNode,
            ColumnAndTableResolver ctr)
    {
        if (planNode instanceof ProjectNode) {
            return buildCiderNode((ProjectNode) planNode, ctr);
        }
        else if (planNode instanceof FilterNode) {
            return buildCiderNode((FilterNode) planNode, ctr);
        }
        else if (planNode instanceof TableScanNode) {
            return buildCiderNode(ctr);
        }
        //FIXME
        throw new RuntimeException("Unsupported operator");
    }

    public static Map<VariableReferenceExpression, ColumnHandle> getColumnHandle(
            PlanNode node)
    {
        return getSourceNode(node).getAssignments();
    }

    public static ConnectorTableHandle getTableHandle(PlanNode node)
    {
        return getSourceNode(node).getTable().getConnectorHandle();
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

    public static String toSchemaJson(ColumnAndTableResolver columnAndTableResolver)
    {
        return ((CiderTableScanNode) buildCiderNode(columnAndTableResolver)).toSchemaStr();
    }

    // DFS visitor, TODO unsupporrted plan should return as fallback
    public static String toRAJsonStr(PlanNode planNode,
            ColumnAndTableResolver columnAndTableResolver)
    {
        return toRAJsonStrHelper(planNode, columnAndTableResolver).toRAJson();
    }
}
