/**
 * Copyright 2011 CaneData.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.canedata.provider.mongodb.expr;

import com.mongodb.client.MongoCollection;
import org.canedata.core.field.expr.AbstractExpressionB;
import org.canedata.core.field.expr.MongoOperator;
import org.canedata.expression.Operator;
import org.canedata.expression.shield.ExpressionA;

import java.util.Arrays;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-5
 */
public abstract class MongoExpressionB extends AbstractExpressionB {
    /**
     * wrapper of $all.
     *
     * @param field
     * @param values
     * @return
     */
    public ExpressionA all(String field, Object...values) {
        if(logger.isDebug())
            logger.debug("Building expression, operator is [all], field is [{0}], value is [{1}].", field, Arrays.toString(values));

        getIntent().step(MongoOperator.ALL, field, values);

        return getExpressionA();
    }

    public ExpressionA other(String key, Object value) {
        if(logger.isDebug())
            logger.debug("Building expression, operator is [other], field is [{0}], value is [{1}].", key, value);

        getIntent().step(MongoOperator.OTHER, key, value);

        return getExpressionA();
    }
}
