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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class CiderFilterCondition
{
    private String op;
    private String type;
    private List<CiderExpression> expressions;

    public CiderFilterCondition(String op, String type, List<CiderExpression> expressions)
    {
        this.op = op;
        this.type = type;
        this.expressions = expressions;
    }

    public List<CiderExpression> getExpressions()
    {
        return expressions;
    }

    public ObjectNode toJson(ObjectMapper objectMapper)
    {
        ObjectNode conditionNode = objectMapper.createObjectNode();
        conditionNode.put("op", matchOperator(op));
        ArrayNode operandsNode = objectMapper.createArrayNode();
        ObjectNode operandsFeilds = objectMapper.createObjectNode();
        ObjectNode operandsInput = objectMapper.createObjectNode();
        for (CiderExpression e : expressions) {
            if (e instanceof CiderConstantExpression) {
                e.toJson(operandsInput, operandsFeilds);
            }
            if (e instanceof CiderVariableReferenceExpression) {
                e.toJson(operandsInput, operandsFeilds);
            }
        }
        operandsNode.add(operandsInput);
        operandsNode.add(operandsFeilds);

        conditionNode.set("operands", operandsNode);

        ObjectNode typeFieldsNode = objectMapper.createObjectNode();
        typeFieldsNode.put("type", type.toUpperCase());
        typeFieldsNode.put("nullable", true);

        conditionNode.set("type", typeFieldsNode);

        return conditionNode;
    }

    private String matchOperator(String originOperator)
    {
        switch (originOperator) {
            case "EQUAL":
                return "=";
            case "GREATER_THAN":
                return ">";
            case "LESS_THAN":
                return "<";
        }
        return originOperator;
    }
}
