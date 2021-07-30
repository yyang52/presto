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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Provide the operator json generator
 */
public abstract class CiderOperatorNode
{
    protected String opName;
    private List<CiderOperatorNode> children = new ArrayList<>();

    public CiderOperatorNode(String opName)
    {
        this.opName = opName;
    }

    public boolean registerChildren(CiderOperatorNode... nodes)
    {
        return children.addAll(Arrays.asList(nodes));
    }

    /**
     * @param objectMapper
     * @param id currently is generated via DFS fashion, it means the tree has to be built in the same way
     * @return
     */
    protected abstract ObjectNode toJson(ObjectMapper objectMapper, String id);

    // Currently DFS visit, TODO(Cheng) need customized vistor???
    public String toRAJson()
    {
        int id = 0;
        LinkedList<CiderOperatorNode> list = new LinkedList<>();
        Set<CiderOperatorNode> visited = new HashSet<>();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode rootNode = objectMapper.createObjectNode();
        ArrayNode relsNode = objectMapper.createArrayNode();
        list.add(this);
        visited.add(this);
        while (!list.isEmpty()) {
            CiderOperatorNode c = list.peek();

            if (!c.children.isEmpty()) {
                boolean isAllVisited = true;
                for (CiderOperatorNode n : c.children) {
                    if (!visited.contains(n)) {
                        list.push(n);
                        visited.add(n);
                        isAllVisited = false;
                        c = n;
                        break;
                    }
                }
                if (!isAllVisited) {
                    continue;
                }
            }
            relsNode.add(c.toJson(objectMapper, String.valueOf(id++)));
            list.pop();
        }
        rootNode.set("rels", relsNode);
        return rootNode.toString();
    }
}
