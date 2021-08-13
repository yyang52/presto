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
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.cider.ColumnAndTableResolver;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Map;

public class HiveColumnAndTableResolver
        extends ColumnAndTableResolver
{
    public HiveColumnAndTableResolver(ConnectorTableHandle connectorTableHandle,
            Map<VariableReferenceExpression, ColumnHandle> vMap)
    {
        super(connectorTableHandle, vMap);
    }

    @Override
    protected void resolveIfNot()
    {
        if (isResolved) {
            return;
        }
        HiveTableHandle hth = (HiveTableHandle) connectorTableHandle;
        this.schemaName = hth.getSchemaName();
        this.tableName = hth.getTableName();
        isResolved = true;
        for (VariableReferenceExpression r : vMap.keySet()) {
            this.fieldNames.add(r.getName());
        }
    }

    @Override
    public int getColumnIndexByColumnHandle(VariableReferenceExpression vre)
    {
        ColumnHandle ch = vMap.get(vre);
        return ((HiveColumnHandle) ch).getHiveColumnIndex();
    }
}
