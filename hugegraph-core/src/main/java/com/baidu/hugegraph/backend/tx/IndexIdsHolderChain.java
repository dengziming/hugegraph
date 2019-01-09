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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.util.E;

public final class IndexIdsHolderChain {

    // Just as an flag
    private final boolean paged;
    private final List<IndexIdsHolder> idsHolders;

    public IndexIdsHolderChain(boolean paged) {
        this.paged = paged;
        this.idsHolders = new ArrayList<>();
    }

    public boolean paged() {
        return this.paged;
    }

    public List<IndexIdsHolder> idsHolders() {
        return this.idsHolders;
    }

    public IndexIdsHolderChain(IndexIdsHolder idsHolder) {
        if (idsHolder instanceof PagedIndexIdsHolder) {
            this.paged = true;
        } else {
            assert idsHolder instanceof EntireIndexIdsHolder;
            this.paged = false;
        }
        this.idsHolders = new ArrayList<>();
        this.idsHolders.add(idsHolder);
    }

    public void link(IndexIdsHolder idsHolder) {
        this.checkIdsHolderType(idsHolder);
        if (this.paged) {
            this.idsHolders.add(idsHolder);
        } else {
            if (this.idsHolders.isEmpty()) {
                this.idsHolders.add(idsHolder);
            } else {
                IndexIdsHolder selfIdsHolder = this.idsHolders.get(0);
                assert selfIdsHolder instanceof EntireIndexIdsHolder;
                EntireIndexIdsHolder holder = (EntireIndexIdsHolder) idsHolder;
                ((EntireIndexIdsHolder) selfIdsHolder).merge(holder);
            }
        }
    }

    public void link(List<IndexIdsHolder> idsHolders) {
        for (IndexIdsHolder idsHolder : idsHolders) {
            this.link(idsHolder);
        }
    }

    public void link(IndexIdsHolderChain chain) {
        E.checkArgument((this.paged && chain.paged) ||
                        (!this.paged && !chain.paged),
                        "Only same IndexIdsHolderChain can be linked");
        this.link(chain.idsHolders());
    }

    private void checkIdsHolderType(IndexIdsHolder idsHolder) {
        if (this.paged) {
            E.checkArgument(idsHolder instanceof PagedIndexIdsHolder,
                            "The IndexIdsHolder to be linked must be " +
                            "PagedIndexIdsHolder in paged mode");
        } else {
            E.checkArgument(idsHolder instanceof EntireIndexIdsHolder,
                            "The IndexIdsHolder to be linked must be " +
                            "EntireIndexIdsHolder in non-paged mode");
        }
    }

    public Iterator<IndexIdsHolder> toIterator() {
        return this.idsHolders.iterator();
    }
}
