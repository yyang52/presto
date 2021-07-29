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

import com.fasterxml.jackson.databind.node.ObjectNode;

class CiderVariableReferenceExpression
        extends CiderExpression
{
    private int columnIndex;
    private String targetType;

    public CiderVariableReferenceExpression(int columnIndex, String targetType)
    {
        this.columnIndex = columnIndex;
        this.targetType = targetType;
    }

    public void toJson(ObjectNode operandsInput, ObjectNode operandsFeilds)
    {
        // column index
        operandsInput.put("input", columnIndex);
        String dataType = matchType(targetType);
        operandsFeilds.put("target_type", matchType(targetType).toUpperCase());
        operandsFeilds.put("type_scale", getTypeScale(targetType));
        operandsFeilds.put("type_precision", getTypePrecision(targetType));
    }
}
