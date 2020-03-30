/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

/**
 * A simple annotation indicating that the class may be safely published to a non-volatile variable, i.e.
 * where both the reading and writing of this variable are non-volatile.
 *
 * This permits both double-checked locking without the volatile modifier for the holder variable,
 * as well as the simple caching factory approach (wherein multiple copies may be instantiated in an initial race).
 *
 * In order to be annotated, a class may either consist exclusively of final and volatile members,
 * or else the remaining members must also be safe for non-volatile publication.
 *
 * TODO: introduce build step to validate each point of use
 */
public @interface SafeForNonVolatilePublication
{
}
