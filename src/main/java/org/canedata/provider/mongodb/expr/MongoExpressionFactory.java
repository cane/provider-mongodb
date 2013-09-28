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

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.canedata.core.intent.Step;
import org.canedata.core.intent.Tracer;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.exception.AnalyzeBehaviourException;
import org.canedata.expression.Expression.Parser;
import org.canedata.expression.Operator;
import org.canedata.expression.ParseExpressionException;
import org.canedata.logging.Logger;

import com.mongodb.BasicDBObject;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-6-20
 */
public interface MongoExpressionFactory {
	public MongoExpressionFactory addExpression(MongoExpression expr);

	public BasicDBObject toQuery() throws ParseExpressionException;

	public BasicDBObject parse(MongoExpression expr)
			throws ParseExpressionException;

	public MongoExpressionFactory reset();

	public class Impl implements MongoExpressionFactory {
		private static final Logger logger = LoggerFactory
				.getLogger(Impl.class);

		final Vector<MongoExpression> exprs = new Vector<MongoExpression>();

		public Impl() {// expression for key

		}

		public MongoExpressionFactory addExpression(MongoExpression expr) {
			exprs.add(expr);

			return this;
		}

		public BasicDBObject toQuery() throws ParseExpressionException {
			if (exprs.isEmpty())
				return null;

			BasicDBObject query = new BasicDBObject();
			for (MongoExpression ce : exprs) {
				parse(ce, query);
			}

			exprs.clear();

			if(logger.isDebug())
                logger.debug("Expression is {0}.", query.toString());

			return query;
		}

		public BasicDBObject parse(MongoExpression expr)
				throws ParseExpressionException {
			BasicDBObject query = new BasicDBObject();
			parse(expr, query);

			if(logger.isDebug())
                logger.debug("Expression is {0}.", query.toString());

			return query;
		}

		protected void parse(MongoExpression exp, final BasicDBObject query)
				throws ParseExpressionException {
			if(logger.isDebug())
                logger.debug("Parsing expression:{0}", exp.toExprString());

			exp.parse(new Parser() {

				public <T> void parse(T t) throws ParseExpressionException {
					MongoExprIntent intent = (MongoExprIntent) t;
					final List<BasicDBObject> ors = new ArrayList<BasicDBObject>();

					try {
						intent.playback(new Tracer() {
							boolean hasOr = false;

							public Tracer trace(final Step step)
									throws AnalyzeBehaviourException {
								if(logger.isDebug())
                                    logger.debug(
										"ExpreIntent#playback, step is {0}, purpose is {1}, values is {2}.",
										step.getName(), step.getPurpose(),
										step.getScalar());

								BasicDBObject innerBdbo = new BasicDBObject();

								switch (step.step()) {
								case Operator.EQUALS:
									innerBdbo.append(step.getPurpose(),
											step.getScalar()[0]);

									break;
								case Operator.NOT_EQUALS:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$ne",
													step.getScalar()[0]));

									break;
								case Operator.LESSTHAN:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$lt",
													step.getScalar()[0]));

									break;
								case Operator.LESSTHAN_OR_EQUAL:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$lte",
													step.getScalar()[0]));

									break;
								case Operator.GREATERTHAN:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$gt",
													step.getScalar()[0]));

									break;
								case Operator.GREATERTHAN_OR_EQUAL:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$gte",
													step.getScalar()[0]));

									break;
								case Operator.BETWEEN:
									innerBdbo.append(
											step.getPurpose(),
											new BasicDBObject()
													.append("$gte",
															step.getScalar()[0])
													.append("$lte",
															step.getScalar()[1]));

									break;
								case Operator.NOT_BETWEEN:
									innerBdbo.append(
											"$or",
											new BasicDBObject[] {
													new BasicDBObject().append(
															step.getPurpose(),
															new BasicDBObject()
																	.append("$lt",
																			step.getScalar()[0])),
													new BasicDBObject().append(
															step.getPurpose(),
															new BasicDBObject()
																	.append("$gt",
																			step.getScalar()[1])) });

									break;
								case Operator.LIKE:
									String likes = (String) step.getScalar()[0];
									innerBdbo.append(step.getPurpose(), Pattern
											.compile(likes,
													Pattern.CASE_INSENSITIVE));

									break;
								case Operator.NOT_LIKE:
									String nlikes = (String) step.getScalar()[0];

									innerBdbo.append(
											step.getPurpose(),
											new BasicDBObject().append(
													"$not",
													Pattern.compile(
															nlikes,
															Pattern.CASE_INSENSITIVE)));
									break;
								case Operator.IN:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$in",
													step.getScalar()));

									break;
								case Operator.NOT_IN:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$nin",
													step.getScalar()));

									break;
								case Operator.EMPTY:
									// {"4up":{$exists:true, $in:["",null]}}
									innerBdbo.append(
											step.getPurpose(),
											new BasicDBObject().append(
													"$exists", true).append(
													"$in",
													new Object[] { "", null }));

									break;
								case Operator.NOT_EMPTY:
									// {"4up":{$exists:true, $ne:"",$ne:null}}
									innerBdbo.append(
											step.getPurpose(),
											new BasicDBObject()
													.append("$exists", true)
													.append("$nin",
															new Object[] { "", null }));

									break;
								case Operator.NULL:
									innerBdbo.append(step.getPurpose(), null);

									break;
								case Operator.NOT_NULL:
									innerBdbo.append(step.getPurpose(),
											new BasicDBObject().append("$ne",
													null));

									break;
								case Operator.MATCH:
									Object matcho = step.getScalar()[0];
									if (matcho instanceof Pattern)
										innerBdbo.append(step.getPurpose(),
												matcho);
									else
										innerBdbo.append(
												step.getPurpose(),
												Pattern.compile(
														(String) matcho,
														Pattern.CASE_INSENSITIVE));

									break;
								case Operator.NOT_MATCH:
									Object nmatcho = step.getScalar()[0];
									if (nmatcho instanceof Pattern)
										innerBdbo.append(step.getPurpose(),
												new BasicDBObject().append(
														"$not", nmatcho));
									else
										innerBdbo.append(
												step.getPurpose(),
												new BasicDBObject().append(
														"$not",
														Pattern.compile(
																(String) nmatcho,
																Pattern.CASE_INSENSITIVE)));

									break;
								case Operator.AND:
									break;
								case Operator.AND_EPR:
									MongoExpression andepr = (MongoExpression) step
											.getScalar()[0];

									Impl.this.parse(andepr, innerBdbo);

									break;
								case Operator.OR:
								case Operator.OR_EPR:
									if(logger.isDebug())
                                        logger.debug(
											"When parsing expression met Operator.OR, The original information is :{0}.",
											innerBdbo.toString());
									if (query.size() == 0)
										throw new ParseExpressionException(
												"The current operation<Operator.OR> does not match the chains of operations.");

									hasOr = true;

									if (step.step() == Operator.OR)
										break;

									MongoExpression oepr = (MongoExpression) step
											.getScalar()[0];

									Impl.this.parse(oepr, innerBdbo);

									break;
								default:
									logger.warn(
											"Step {0} does not apply to activities query, this step will be ignored.",
											step.getName());
								}

								if (innerBdbo.isEmpty())
									return this;

								if (hasOr) {
									hasOr = false;

									ors.add(innerBdbo);
								} else {
									query.putAll((BSONObject) innerBdbo);
								}

								return this;
							}

						});
						
						//ignore or operations.
						if(ors.isEmpty())
							return;
						
						BasicDBObject no = new BasicDBObject(query.toMap());
						query.clear();

						ors.add(0, no);
						query.append("$or", ors.toArray());
					} catch (AnalyzeBehaviourException e) {
						throw new ParseExpressionException(e);
					}

				}

			});
		}

		public MongoExpressionFactory reset() {
			exprs.clear();

			return this;
		}

	}
}
