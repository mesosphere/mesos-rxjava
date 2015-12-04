/*
 *    Copyright (C) 2015 Mesosphere, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mesosphere.mesos.rx.java.util;

import org.junit.Test;

import java.util.ArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.unmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

public final class CollectionUtilsTest {

    @Test
    public void deepEquals_bothDistinctUnmodifiableArrayList() throws Exception {
        assertThat(CollectionUtils.deepEquals(
            unmodifiableList(newArrayList(b(1), b(2), b(3))),
            unmodifiableList(newArrayList(b(1), b(2), b(3)))
        )).isTrue();
    }

    @Test
    public void deepEquals_unmodifiableArrayListVsArrayList() throws Exception {
        assertThat(CollectionUtils.deepEquals(
            unmodifiableList(newArrayList(b(1), b(2), b(3))),
            newArrayList(b(1), b(2), b(3))
        )).isTrue();
    }

    @Test
    public void deepEquals_arrayListVsUnmodifiableArrayList() throws Exception {
        assertThat(CollectionUtils.deepEquals(
            newArrayList(b(1), b(2), b(3)),
            unmodifiableList(newArrayList(b(1), b(2), b(3)))
        )).isTrue();
    }

    @Test
    public void deepEquals_arrayListVsArrayList() throws Exception {
        assertThat(CollectionUtils.deepEquals(
            newArrayList(b(1), b(2), b(3)),
            newArrayList(b(1), b(2), b(3))
        )).isTrue();
    }

    @Test
    public void deepEquals_sameList() throws Exception {
        final ArrayList<byte[]> list = newArrayList(b(1), b(2), b(3));
        assertThat(CollectionUtils.deepEquals(
            list, list
        )).isTrue();
    }

    @Test
    public void deepEquals_differentSizeLists() throws Exception {
        assertThat(CollectionUtils.deepEquals(
            newArrayList(b(1), b(2), b(3), b(4)),
            newArrayList(b(1), b(2), b(3))
        )).isFalse();
    }

    @Test
    public void deepEquals_differentContents() throws Exception {
        assertThat(CollectionUtils.deepEquals(
            newArrayList(b(11), b(12), b(13)),
            newArrayList(b(1), b(2), b(3))
        )).isFalse();
    }

    private static byte[] b(final int b) {
        return new byte[]{(byte) b};
    }
}
