// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package rawkv_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	rawclient "github.com/pingcap/tidb/br/pkg/restore/internal/rawkv"
	brutils "github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/rawkv"
)

// fakeRawkvClient is a mock for rawkv.client
type fakeRawkvClient struct {
	rawkv.Client
	kvs []kv.Entry
}

func newFakeRawkvClient() *fakeRawkvClient {
	return &fakeRawkvClient{
		kvs: make([]kv.Entry, 0),
	}
}

func (f *fakeRawkvClient) BatchPut(
	ctx context.Context,
	keys [][]byte,
	values [][]byte,
	options ...rawkv.RawOption,
) error {
	if len(keys) != len(values) {
		return errors.Annotate(berrors.ErrInvalidArgument,
			"the length of keys don't equal the length of values")
	}

	for i := range keys {
		entry := kv.Entry{
			Key:   keys[i],
			Value: values[i],
		}
		f.kvs = append(f.kvs, entry)
	}
	return nil
}

func (f *fakeRawkvClient) Close() error {
	return nil
}

func TestRawKVBatchClient(t *testing.T) {
	fakeRawkvClient := newFakeRawkvClient()
	batchCount := 3
	rawkvBatchClient := rawclient.NewRawKVBatchClient(fakeRawkvClient, batchCount)
	defer rawkvBatchClient.Close()

	rawkvBatchClient.SetColumnFamily("default")

	kvs := []kv.Entry{
		{Key: codec.EncodeUintDesc([]byte("key1"), 1), Value: []byte("v1")},
		{Key: codec.EncodeUintDesc([]byte("key2"), 2), Value: []byte("v2")},
		{Key: codec.EncodeUintDesc([]byte("key3"), 3), Value: []byte("v3")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 4), Value: []byte("v4")},
		{Key: codec.EncodeUintDesc([]byte("key5"), 5), Value: []byte("v5")},
	}

	for i := range batchCount {
		require.Equal(t, 0, len(fakeRawkvClient.kvs))
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
	}
	require.Equal(t, batchCount, len(fakeRawkvClient.kvs))

	for i := batchCount; i < len(kvs); i++ {
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
	}
	require.Equal(t, batchCount, len(fakeRawkvClient.kvs))
	err := rawkvBatchClient.PutRest(context.TODO())
	require.Nil(t, err)
	sort.Slice(fakeRawkvClient.kvs, func(i, j int) bool {
		return bytes.Compare(fakeRawkvClient.kvs[i].Key, fakeRawkvClient.kvs[j].Key) < 0
	})
	require.Equal(t, kvs, fakeRawkvClient.kvs)
}

func TestRawKVBatchClientDuplicated(t *testing.T) {
	fakeRawkvClient := newFakeRawkvClient()
	batchCount := 3
	rawkvBatchClient := rawclient.NewRawKVBatchClient(fakeRawkvClient, batchCount)
	defer rawkvBatchClient.Close()

	rawkvBatchClient.SetColumnFamily("default")

	kvs := []kv.Entry{
		{Key: codec.EncodeUintDesc([]byte("key1"), 1), Value: []byte("v1")},
		{Key: codec.EncodeUintDesc([]byte("key1"), 2), Value: []byte("v2")},
		{Key: codec.EncodeUintDesc([]byte("key3"), 3), Value: []byte("v3")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 4), Value: []byte("v4")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 5), Value: []byte("v5")},
	}

	expectedKvs := []kv.Entry{
		// we keep the large ts entry, and we only make sure there is no duplicated entry in a batch.
		// which is 3. so the duplicated key4 not in a batch will have two versions finally.
		{Key: codec.EncodeUintDesc([]byte("key1"), 2), Value: []byte("v2")},
		{Key: codec.EncodeUintDesc([]byte("key3"), 3), Value: []byte("v3")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 5), Value: []byte("v5")},
		{Key: codec.EncodeUintDesc([]byte("key4"), 4), Value: []byte("v4")},
	}

	for i := range batchCount {
		require.Equal(t, 0, len(fakeRawkvClient.kvs))
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
	}
	// There only two different keys which doesn't send to kv.
	require.Equal(t, 0, len(fakeRawkvClient.kvs))

	for i := batchCount; i < 5; i++ {
		err := rawkvBatchClient.Put(context.TODO(), kvs[i].Key, kvs[i].Value, uint64(i+1))
		require.Nil(t, err)
		require.Equal(t, batchCount, len(fakeRawkvClient.kvs))
	}

	err := rawkvBatchClient.PutRest(context.TODO())
	require.Nil(t, err)
	sort.Slice(fakeRawkvClient.kvs, func(i, j int) bool {
		return bytes.Compare(fakeRawkvClient.kvs[i].Key, fakeRawkvClient.kvs[j].Key) < 0
	})
	require.Equal(t, expectedKvs, fakeRawkvClient.kvs)
}

// TestRawKVBatchClientDedupConcernCase1 demonstrates the pre-existing concern
// raised in https://github.com/pingcap/tidb/pull/67268 by @Leavrth (Case 1):
//
// When the DefaultCF batch client deduplicates multiple MVCC versions of the
// same logical meta key by highest-TS, it may retain an entry whose start_ts
// corresponds to an uncommitted prewrite — i.e. the WriteCF has a Rollback for
// that commit_ts rather than a Put.
//
// Example:
//
//	DefaultCF: k1@100 (prewrite-1), k1@120 (prewrite-2)
//	WriteCF:   k1@110:start_ts=100 → Put, k1@130:start_ts=120 → Rollback
//
// The DefaultCF batch collapses to k1@120 (highest TS wins). But k1@120 is the
// prewrite for start_ts=120, whose WriteCF entry is a Rollback@130. After
// restore, TiKV's DefaultCF contains k1@120 (dangling prewrite value) while
// the corresponding committed write is the Put at commit_ts=110 referencing
// start_ts=100 — which was dropped.
//
// This is a pre-existing behavior of RawKVBatchClient.Put independent of any
// upstream per-file deduplication.
func TestRawKVBatchClientDedupConcernCase1(t *testing.T) {
	fakeClient := newFakeRawkvClient()
	// Large capacity so no mid-batch flush; all dedup happens within Put.
	batchClient := rawclient.NewRawKVBatchClient(fakeClient, 100)
	defer batchClient.Close()
	batchClient.SetColumnFamily("default")

	ctx := context.Background()
	dbKey := meta.DBkey(1)
	tableField := meta.TableKey(42)

	// Two DefaultCF prewrites for the same logical meta key at ts=100 and ts=120.
	// k1@100 corresponds to the committed Put (WriteCF commit_ts=110, start_ts=100).
	// k1@120 corresponds to the rolled-back prewrite (WriteCF commit_ts=130, rollback).
	keyAt100 := brutils.EncodeTxnMetaKey(dbKey, tableField, 100)
	keyAt120 := brutils.EncodeTxnMetaKey(dbKey, tableField, 120)

	require.NoError(t, batchClient.Put(ctx, keyAt100, []byte("committed-value"), 100))
	require.NoError(t, batchClient.Put(ctx, keyAt120, []byte("rolled-back-value"), 120))
	require.NoError(t, batchClient.PutRest(ctx))

	// The batch client kept only ts=120 (highest TS wins). The committed value
	// at ts=100 was silently dropped. After restore, DefaultCF contains the
	// rolled-back prewrite's value, not the committed value.
	require.Len(t, fakeClient.kvs, 1, "highest-TS dedup: only one entry survives per logical key")
	require.True(t, bytes.Equal(keyAt120, fakeClient.kvs[0].Key), "ts=120 (rolled-back prewrite) survived; ts=100 (committed value) was dropped")
	require.Equal(t, []byte("rolled-back-value"), fakeClient.kvs[0].Value)
}

// TestRawKVBatchClientDedupConcernCase2 demonstrates the pre-existing concern
// raised in https://github.com/pingcap/tidb/pull/67268 by @Leavrth (Case 2):
//
// DefaultCF and WriteCF are restored via independent RawKVBatchClient instances.
// Each independently deduplicates by logical key (highest TS wins). Because
// WriteCF commit_ts ordering is independent of DefaultCF start_ts ordering,
// the surviving WriteCF entry may reference a DefaultCF start_ts that was
// dropped by the DefaultCF dedup.
//
// Example:
//
//	DefaultCF: k1@100 (prewrite-1), k1@120 (prewrite-2)
//	WriteCF:   k1@130:start_ts=120 → Put, k1@140:start_ts=100 → Put
//
// DefaultCF dedup (independent): k1@120 wins (dropped k1@100).
// WriteCF dedup (independent):   k1@140:start_ts=100 wins (dropped k1@130:start_ts=120).
//
// Result: WriteCF says the committed value lives in DefaultCF at start_ts=100,
// but DefaultCF no longer has k1@100 — it was collapsed away. The read would
// return an empty value even though two Put commits existed.
//
// This is a pre-existing behavior of independent per-CF RawKVBatchClient
// instances independent of any upstream per-file deduplication.
func TestRawKVBatchClientDedupConcernCase2(t *testing.T) {
	fakeDefaultCF := newFakeRawkvClient()
	fakeWriteCF := newFakeRawkvClient()

	defaultBatch := rawclient.NewRawKVBatchClient(fakeDefaultCF, 100)
	defer defaultBatch.Close()
	defaultBatch.SetColumnFamily("default")

	writeBatch := rawclient.NewRawKVBatchClient(fakeWriteCF, 100)
	defer writeBatch.Close()
	writeBatch.SetColumnFamily("write")

	ctx := context.Background()
	dbKey := meta.DBkey(2)
	tableField := meta.TableKey(99)

	// DefaultCF: two prewrites for the same logical key.
	// start_ts=100: prewrite-1 (committed via WriteCF@140)
	// start_ts=120: prewrite-2 (committed via WriteCF@130)
	keyDefaultAt100 := brutils.EncodeTxnMetaKey(dbKey, tableField, 100)
	keyDefaultAt120 := brutils.EncodeTxnMetaKey(dbKey, tableField, 120)

	require.NoError(t, defaultBatch.Put(ctx, keyDefaultAt100, []byte("value-for-start100"), 100))
	require.NoError(t, defaultBatch.Put(ctx, keyDefaultAt120, []byte("value-for-start120"), 120))
	require.NoError(t, defaultBatch.PutRest(ctx))

	// WriteCF: two commits for the same logical key with cross-CF references.
	// commit_ts=130 references start_ts=120 (prewrite-2)
	// commit_ts=140 references start_ts=100 (prewrite-1); higher commit_ts wins
	keyWriteAt130 := brutils.EncodeTxnMetaKey(dbKey, tableField, 130)
	keyWriteAt140 := brutils.EncodeTxnMetaKey(dbKey, tableField, 140)

	require.NoError(t, writeBatch.Put(ctx, keyWriteAt130, []byte("write-ref-start120"), 130))
	require.NoError(t, writeBatch.Put(ctx, keyWriteAt140, []byte("write-ref-start100"), 140))
	require.NoError(t, writeBatch.PutRest(ctx))

	// DefaultCF result: only ts=120 survived (highest TS). start_ts=100 was dropped.
	require.Len(t, fakeDefaultCF.kvs, 1)
	require.True(t, bytes.Equal(keyDefaultAt120, fakeDefaultCF.kvs[0].Key), "DefaultCF kept ts=120; ts=100 (referenced by WriteCF winner) was dropped")

	// WriteCF result: only commit_ts=140 survived (highest TS).
	// But commit_ts=140 references start_ts=100 which no longer exists in DefaultCF.
	require.Len(t, fakeWriteCF.kvs, 1)
	require.True(t, bytes.Equal(keyWriteAt140, fakeWriteCF.kvs[0].Key), "WriteCF kept commit_ts=140 (start_ts=100); its DefaultCF prewrite was silently dropped")

	// The cross-CF reference is now broken: reading the logical key via WriteCF
	// would resolve to DefaultCF@100, which is absent. The committed value from
	// the earlier transaction (start_ts=100) is lost.
}
