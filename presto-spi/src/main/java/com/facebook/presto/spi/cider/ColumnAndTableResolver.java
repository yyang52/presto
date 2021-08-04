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
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Map;

public abstract class ColumnAndTableResolver
{
    protected boolean isResolved;
    protected String schemaName;
    protected String tableName;
    protected List<String> fieldNames;
    protected ConnectorTableHandle connectorTableHandle;
    protected Map<VariableReferenceExpression, ColumnHandle> vMap;

    public ColumnAndTableResolver(ConnectorTableHandle connectorTableHandle,
            Map<VariableReferenceExpression, ColumnHandle> vMap)
    {
        this.connectorTableHandle = connectorTableHandle;
        this.vMap = vMap;
        this.isResolved = false;
    }

    protected abstract void resolveIfNot();

    public String getSchema()
    {
        resolveIfNot();
        return this.schemaName;
    }

    public String getTableName()
    {
        resolveIfNot();
        return this.tableName;
    }

    public List<String> getFields()
    {
        resolveIfNot();
        return this.fieldNames;
    }

    public abstract int getColumnIndexByColumnHandle(VariableReferenceExpression vre);
}
