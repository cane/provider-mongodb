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

import org.bson.Document;
import org.canedata.core.intent.Step;
import org.canedata.core.intent.Tracer;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.intent.Limiter;
import org.canedata.core.util.StringUtils;
import org.canedata.exception.AnalyzeBehaviourException;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.expr.MongoExpression;
import org.canedata.provider.mongodb.expr.MongoExpressionFactory;
import org.canedata.provider.mongodb.intent.MongoStep;

import java.io.Closeable;

/**
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-9
 */
public class IntentParser {
    protected static final Logger logger = LoggerFactory
            .getLogger(IntentParser.class);

    public static interface ParserResult extends Closeable {
        public Document genericFields();

        public boolean hasGenericFields();

        public Document operationFields();

        public Document projections();

        public boolean hasProjections();

        public Limiter limiter();

        public Document sorter();

        public boolean hasSorter();

        public Document options();

        public MongoExpressionFactory exprFactory();

        public boolean hasQuery();

        public void close();
    }

    //@FunctionalInterface
    public static interface Injector {
        /**
         * @param step default return false, the default processing flow will be interrupted.
         * @return
         */
        public boolean accept(Step step);
    }

    public static ParserResult parse(MongoEntity entity, Injector injector) throws AnalyzeBehaviourException {
        final MongoExpressionFactory expFactory = new MongoExpressionFactory.Impl();
        final Document genericFields = new Document();
        final Document operationFields = new Document();
        final Document projection = new Document();
        final Limiter limiter = new Limiter.Default();
        final Document sorter = new Document();
        final Document options = new Document();
        options.append(Options.RETAIN, false)
                .append(Options.UPSERT, false).append(Options.BATCH_SIZE, 20);

        entity.getIntent().playback(new Tracer() {

            public Tracer trace(Step step) throws AnalyzeBehaviourException {
                // Allow interrupt the processing
                if (null != injector && injector.accept(step))
                    return this;

                switch (step.step()) {
                    case MongoStep.FILTER:
                        logger.debug("Putting filter ...");

                        if (null != step.getScalar())
                            expFactory.addExpression((MongoExpression) step
                                    .getScalar()[0]);

                        break;
                    case MongoStep.PROJECTION:
                        if("doc".equals(step.getPurpose())){
                            projection.putAll((Document)step.getScalar()[0]);
                        } else {
                            for (Object field : step.getScalar()) {
                                String f = (String) field;

                                logger.debug("Projected field {0} ...", f);

                                projection.append(f, 1);
                            }
                        }

                        break;
                    case MongoStep.LIMIT:
                        if (null == limiter)
                            break;

                        limiter.reset();

                        if (step.getScalar().length == 2)
                            limiter.limit((Integer) step.getScalar()[0],
                                    (Integer) step.getScalar()[1]);
                        else
                            limiter.count((Integer) step.getScalar()[0]);

                        break;
                    case MongoStep.ORDER:
                        if (null == sorter)
                            break;

                        int ord = 1;
                        if (step.getPurpose() != null)
                            ord = -1;

                        String[] ordFs = (String[]) step.getScalar();
                        for (String f : ordFs) {
                            sorter.append(f, ord);
                        }
                        break;

                    case MongoStep.PUT:
                        if (StringUtils.isBlank(step.getPurpose()))
                            break;

                        Object val = (step.getScalar() == null || step.getScalar().length == 0) ? null
                                : step.getScalar()[0];

                        if (step.getPurpose().matches(MongoEntity.internalCmds))
                            operationFields.append(step.getPurpose(), val);
                        else
                            genericFields.append(step.getPurpose(), val);

                        break;
                    case MongoStep.OPTION:
                        Intents.parseOption(step, options);
                        break;
                    default:
                        logger.warn(
                                "Step {0} does not apply to activities query, this step will be ignored.",
                                step.getName());
                }

                return this;
            }

        });

        return new ParserResult() {
            @Override
            public Document genericFields() {
                return genericFields;
            }

            @Override
            public Document operationFields() {
                return operationFields;
            }

            @Override
            public Document projections() {
                return projection;
            }

            @Override
            public Limiter limiter() {
                return limiter;
            }

            @Override
            public Document sorter() {
                return sorter;
            }

            @Override
            public Document options() {
                return options;
            }

            @Override
            public MongoExpressionFactory exprFactory() {
                return expFactory;
            }

            @Override
            public boolean hasGenericFields() {
                return !genericFields.isEmpty();
            }

            @Override
            public boolean hasProjections() {
                return !projection.isEmpty();
            }

            @Override
            public boolean hasSorter() {
                return !sorter.isEmpty();
            }

            @Override
            public boolean hasQuery() {
                return expFactory.hasQuery();
            }

            @Override
            public void close() {
                if (!options.containsKey(Options.RETURN_NEW) || !options.getBoolean(Options.RETURN_NEW))
                    entity.getIntent().reset();

                genericFields.clear();
                operationFields.clear();
                options.clear();
                sorter.clear();
                projection.clear();
                expFactory.reset();
            }
        };
    }

    public static class Intents {
        public static void parseOption(Step step, Document options) {
            Object oVal = Options.ADD_CODEC.equals(step.getPurpose()) ? step.getScalar() : step.getScalar()[0];
            options.append(step.getPurpose(), oVal);
        }
    }
}
