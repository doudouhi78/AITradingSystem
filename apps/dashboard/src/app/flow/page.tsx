'use client';

import { DashboardShell } from '@/components/dashboard-shell';
import { ErrorState } from '@/components/error-state';
import { LoadingState } from '@/components/loading-state';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useDashboardQuery } from '@/hooks/use-dashboard-query';
import { FlowView } from '@/lib/types';

export default function FlowPage() {
  const { data, loading, error } = useDashboardQuery<FlowView>('/api/v1/flow');
  return (
    <DashboardShell>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-semibold tracking-tight">流转与问题</h2>
          <p className="mt-2 text-sm text-muted">看最近 trace、卡点、缺证据项和流转状态。</p>
        </div>
        {loading ? <LoadingState /> : null}
        {error ? <ErrorState message={error} /> : null}
        {data ? (
          <div className="grid gap-6 xl:grid-cols-2">
            <Card>
              <CardHeader><CardTitle>最近 Trace</CardTitle></CardHeader>
              <CardContent>
                <ul className="space-y-3 text-sm">
                  {data.recent_traces.map((item, index) => (
                    <li key={`${String(item.run_id)}-${index}`} className="rounded-xl border border-border p-3">
                      <div className="font-medium">{String(item.run_id)}</div>
                      <div className="text-muted">{String(item.experiment_id)}</div>
                    </li>
                  ))}
                </ul>
              </CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle>最近打回/卡点</CardTitle></CardHeader>
              <CardContent>
                <pre className="overflow-x-auto whitespace-pre-wrap text-sm">{JSON.stringify(data.recent_returns, null, 2)}</pre>
              </CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle>阶段停滞项</CardTitle></CardHeader>
              <CardContent>
                <pre className="overflow-x-auto whitespace-pre-wrap text-sm">{JSON.stringify(data.blocked_items, null, 2)}</pre>
              </CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle>缺证据项</CardTitle></CardHeader>
              <CardContent>
                <pre className="overflow-x-auto whitespace-pre-wrap text-sm">{JSON.stringify(data.missing_evidence_items, null, 2)}</pre>
              </CardContent>
            </Card>
          </div>
        ) : null}
      </div>
    </DashboardShell>
  );
}
