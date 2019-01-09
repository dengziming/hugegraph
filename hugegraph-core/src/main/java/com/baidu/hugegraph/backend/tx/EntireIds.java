/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.tx;

import java.util.Collection;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.util.E;

public class EntireIds implements Ids {

    private final Set<Id> ids;

    public EntireIds(Collection<Id> ids) {
        E.checkArgument(ids instanceof Set,
                        "The ids of EntireIds must be Set, but got '%s'",
                        ids.getClass().getName());
        this.ids = (Set<Id>) ids;
    }

    public EntireIds(Set<Id> ids) {
        this.ids = ids;
    }

    public Set<Id> ids() {
        return this.ids;
    }
}
