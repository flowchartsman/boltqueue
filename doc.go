// Copyright (c) 2015 Andy Walker.
// Use of this source code is governed by the MIT License that can be
// found in the LICENSE file.

/*
Package boltqueue provides a persistent queue or priority queue based on
boltdb (https://github.com/boltdb/bolt)

Priority Queue

boltqueue's PQueue type represents a priority queue. Messages may be
inserted into the queue at a numeric priority between 0(highest) and
255(lowest). Messages are dequeued following priority order, then time
ordering, with the oldest messages of the highest priority emerging
first.
*/
package boltqueue
