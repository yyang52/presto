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
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CiderFilterNode
        extends CiderOperatorNode
{
    private CiderFilterCondition condition;

    public CiderFilterNode(CiderFilterCondition condition)
    {
        super("LogicalFilter");
        this.condition = condition;
    }

    @Override
    protected ObjectNode toJson(ObjectMapper objectMapper, String id)
    {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("id", id);
        objectNode.put("relOp", "LogicalFilter");
        objectNode.set("condition", condition.toJson(objectMapper));
        return objectNode;
    }
}
