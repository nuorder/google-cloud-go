package spanner

import (
	"context"

	"cloud.google.com/go/internal/trace"
	"github.com/googleapis/gax-go/v2"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// UpdateWithResultSet .update but returns original resultSet which contains Stats
func (t *ReadWriteTransaction) UpdateWithResultSet(ctx context.Context, stmt Statement, opts QueryOptions) (resultSet *sppb.ResultSet, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/spanner.Update")
	defer func() { trace.EndSpan(ctx, err) }()
	req, sh, err := t.prepareExecuteSQL(ctx, stmt, opts)
	if err != nil {
		return nil, err
	}
	hasInlineBeginTransaction := false
	if _, ok := req.GetTransaction().GetSelector().(*sppb.TransactionSelector_Begin); ok {
		hasInlineBeginTransaction = true
	}
	var md metadata.MD
	resultSet, err = sh.getClient().ExecuteSql(contextWithOutgoingMetadata(ctx, sh.getMetadata(), t.disableRouteToLeader), req, gax.WithGRPCOptions(grpc.Header(&md)))
	if getGFELatencyMetricsFlag() && md != nil && t.ct != nil {
		if err := createContextAndCaptureGFELatencyMetrics(ctx, t.ct, md, "update"); err != nil {
			trace.TracePrintf(ctx, nil, "Error in recording GFE Latency. Try disabling and rerunning. Error: %v", err)
		}
	}
	if err != nil {
		if hasInlineBeginTransaction {
			t.setTransactionID(nil)
			return nil, errInlineBeginTransactionFailed()
		}
		return nil, ToSpannerError(err)
	}
	if hasInlineBeginTransaction {
		if resultSet != nil && resultSet.GetMetadata() != nil && resultSet.GetMetadata().GetTransaction() != nil &&
			resultSet.GetMetadata().GetTransaction().GetId() != nil {
			t.setTransactionID(resultSet.GetMetadata().GetTransaction().GetId())
		} else {
			//  retry with explicit begin transaction
			t.setTransactionID(nil)
			return nil, errInlineBeginTransactionFailed()
		}
	}
	if resultSet.Stats == nil {
		return nil, spannerErrorf(codes.InvalidArgument, "query passed to Update: %q", stmt.SQL)
	}
	return resultSet, nil
}
