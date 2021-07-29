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

public abstract class CiderExpression
{
    public abstract void toJson(ObjectNode operandsInput, ObjectNode operandsFeilds);

    protected String matchType(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "Int":
                return "Integer";
            case "double":
                return "decimal";
        }
        return type;
    }

    protected int getTypeScale(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "Int":
                return 0;
            case "double":
                return 2;
        }
        return -1;
    }

    protected int getTypePrecision(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "Int":
                return 10;
            case "double":
                return 5;
        }
        return -1;
    }
}
