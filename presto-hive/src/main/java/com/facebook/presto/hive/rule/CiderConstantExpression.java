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

import java.math.BigDecimal;

public class CiderConstantExpression
        extends CiderExpression
{
    private String type;
    private String value;

    public CiderConstantExpression(String type, String value)
    {
        this.type = type;
        this.value = value;
    }

    private int getIntLiteral(int scale, double literal)
    {
        while (scale >= 1) {
            literal = literal * 10;
            scale--;
        }
        return (int) literal;
    }

    @Override
    public void toJson(ObjectNode operandsInput, ObjectNode operandsFeilds)
    {
        int dScale = 0;
        int dPrecision = 0;
        // FIXME why switch, how about other types?
        //  constant will be taken as DECIMAL
        switch (type) {
            case "integer":
            case "double":
                double doubleLiteral = Double.parseDouble(value);
                BigDecimal decimalLiteral = new BigDecimal(doubleLiteral);
                dScale = decimalLiteral.scale();
                dPrecision = decimalLiteral.precision();
                operandsFeilds.put("literal", getIntLiteral(dScale, doubleLiteral));
        }
        // no matter integer or double, will convert the constant to DECIMAL
        operandsFeilds.put("type", "DECIMAL");
        operandsFeilds.put("scale", dScale);
        operandsFeilds.put("precision", dPrecision);

        operandsFeilds.put("type_scale", dScale);
        operandsFeilds.put("type_precision", dPrecision);
    }
}
