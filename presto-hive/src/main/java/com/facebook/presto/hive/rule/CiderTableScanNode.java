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

import java.util.ArrayList;
import java.util.List;

class CiderTableScanNode
        extends CiderOperatorNode
{
    private String tableName;
    private String schemaName;
    private List<String> fieldNames;
    private List<String> inputs = new ArrayList<>();

    public CiderTableScanNode(String schema, String tableName, List<String> fieldNames)
    {
        super("LogicalTableScan");
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.schemaName = schema;
    }

    @Override
    protected ObjectNode toJson(ObjectMapper objectMapper, String id)
    {
        ObjectNode tableScanJsonNode = objectMapper.createObjectNode();
        tableScanJsonNode.put("id", id);
        tableScanJsonNode.put("relOp", opName);
        ArrayNode fieldsNode = objectMapper.createArrayNode();
        for (String f : fieldNames) {
            fieldsNode.add(f);
        }
        tableScanJsonNode.set("fieldNames", fieldsNode);
        ArrayNode tableNode = objectMapper.createArrayNode();
        tableNode.add(schemaName);
        tableNode.add(tableName);
        tableScanJsonNode.set("table", tableNode);
        ArrayNode inputNode = objectMapper.createArrayNode();
        tableScanJsonNode.set("inputs", inputNode);
        return tableScanJsonNode;
    }
}
