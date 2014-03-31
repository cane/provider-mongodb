/*
 * Copyright (c) 2013 CaneData.org
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

package org.canedata.provider.mongodb.entity;

import org.canedata.core.logging.LoggerFactory;
import org.canedata.entity.Batch;
import org.canedata.entity.Entity;
import org.canedata.expression.Expression;
import org.canedata.field.Field;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.intent.MongoIntent;
import org.canedata.provider.mongodb.intent.MongoStep;

import java.util.Map;

/**
 * <pre>
 *     Entity e = ...
 *     e.batch().put(...).put(...).add().put(...).put(...).add().create();
 *     e.batch().put(...).put(...).add(expr).put(...).put(...).add(expr).update();
 * </pre>
 * @author Sun Yat-ton
 * @version 1.00.000 2013-10-08
 */
public abstract class MongoBatch implements Batch {
    private static final Logger logger = LoggerFactory.getLogger(MongoBatch.class);

    abstract MongoIntent getIntent();

    public Batch put(String field, Object value) {
        logger.debug("Put value {0} to {1}.", value, field);

        getIntent().step(MongoStep.PUT, field, value);

        return this;
    }

    public Batch putAll(Map<String, Object> values) {
        if (null == values || values.isEmpty())
            return this;

        for (Map.Entry<String, Object> e : values.entrySet()) {
            put(e.getKey(), e.getValue());
        }

        return this;
    }

    public BatchAction add() {
        if(logger.isDebug())
            logger.debug("Add batch ...");

        getIntent().step(MongoStep.BATCH, null);

        return null;
    }

    public BatchAction add(Expression expr) {
        if(logger.isDebug())
            logger.debug("Add batch, expression is {0} ...", expr.toExprString());

        getIntent().step(MongoStep.BATCH, "expr", expr);

        return null;
    }

    public Batch clear() {
        logger.debug("Clearing added batch ...");

        getIntent().step(MongoStep.BATCH_CLEAR, null);

        return this;
    }

}
