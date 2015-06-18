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
package org.apache.cassandra.db.rows;

import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * Static utilities to work on RangeTombstoneMarker objects.
 */
public abstract class RangeTombstoneMarkers
{
    private RangeTombstoneMarkers() {}

    // TODO: move to RangeTombstoneMarker? seems like it doesn't make much sense to keep this one inner class on its own in a static utilities container class
    public static class Merger
    {
        private final CFMetaData metadata;
        private final UnfilteredRowIterators.MergeListener listener;
        private final DeletionTime partitionDeletion;

        private Slice.Bound bound;
        private final RangeTombstoneMarker[] markers;

        // Stores for each iterator, what is the currently open marker
        private final DeletionTimeArray openMarkers;
        private final DeletionTimeArray.Cursor openMarkersCursor = new DeletionTimeArray.Cursor();

        // The index in openMarkers of the "biggest" marker. This is the last open marker
        // that has been returned for the merge.
        private int openMarker = -1;

        // As reusable marker to return the result
        private final ReusableRangeTombstoneMarker reusableMarker;

        public Merger(CFMetaData metadata, int size, DeletionTime partitionDeletion, UnfilteredRowIterators.MergeListener listener)
        {
            this.metadata = metadata;
            this.listener = listener;
            this.partitionDeletion = partitionDeletion;

            this.markers = new RangeTombstoneMarker[size];
            this.openMarkers = new DeletionTimeArray(size);
            this.reusableMarker = new ReusableRangeTombstoneMarker(metadata.clusteringColumns().size());
        }

        public void clear()
        {
            Arrays.fill(markers, null);
            bound = null;
        }

        public void add(int i, RangeTombstoneMarker marker)
        {
            assert bound == null || bound.equals(marker.clustering()) : bound.toString(metadata) + " " + marker.clustering().toString(metadata);
            bound = marker.clustering();
            markers[i] = marker;
        }

        public Unfiltered merge()
        {
            if (bound.kind().isStart())
                return mergeOpenMarkers();
            else
                return mergeCloseMarkers();
        }

        private Unfiltered mergeOpenMarkers()
        {
            int toReturn = -1;
            for (int i = 0; i < markers.length; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                // We can completely ignore any marker that is shadowed by a partition level deletion
                if (partitionDeletion.supersedes(marker.deletionTime()))
                    continue;

                // We have an open marker. It's only present after merge if it's bigger than the
                // currently open marker.
                DeletionTime dt = marker.deletionTime();
                openMarkers.set(i, dt);

                if (openMarker < 0 || !openMarkers.supersedes(openMarker, dt))
                    openMarker = toReturn = i;
            }

            if (toReturn < 0)
                return null;

            openMarkersCursor.setTo(openMarkers, toReturn);
            if (listener != null)
                listener.onMergedRangeTombstoneMarkers(bound, openMarkersCursor, markers);
            return reusableMarker.setTo(bound, openMarkersCursor);
        }

        private Unfiltered mergeCloseMarkers()
        {
            RangeTombstoneMarker closed = markers[openMarker];

            if (listener != null)
                listener.onMergedRangeTombstoneMarkers(bound, closed.deletionTime(), markers);

            for (int i = 0; i < markers.length; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                openMarkersCursor.setTo(openMarkers, i);
                assert openMarkersCursor.equals(marker.deletionTime());
                openMarkers.clear(i);
            }

            // We've cleaned all of the open markers so updateOpenMarker will return the new biggest one
            updateOpenMarker();

            if (openMarker >= 0)
            {
                // if we are on a DeletionTime boundary, just send an open marker

                openMarkersCursor.setTo(openMarkers, openMarker);
                Slice.Bound openingBound = bound.withNewKind(bound.kind().invert());
                if (listener != null)
                    listener.onMergedRangeTombstoneMarkers(openingBound, openMarkersCursor, markers);

                return reusableMarker.setTo(openingBound, openMarkersCursor);
            }
            else
            {
                // if there is no new RT, just send a close marker
                return reusableMarker.setTo(bound, closed.deletionTime());
            }
        }

        public DeletionTime activeDeletion()
        {
            // Note that we'll only have an openMarker if it supersedes the partition deletion
            return openMarker < 0 ? partitionDeletion : openMarkersCursor.setTo(openMarkers, openMarker);
        }

        private void updateOpenMarker()
        {
            openMarker = -1;
            for (int i = 0; i < openMarkers.size(); i++)
            {
                if (openMarkers.isLive(i) && (openMarker < 0 || openMarkers.supersedes(i, openMarker)))
                    openMarker = i;
            }
        }
    }
}
