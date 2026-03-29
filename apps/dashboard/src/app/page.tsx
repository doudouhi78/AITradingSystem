'use client';

import Link from 'next/link';

import { DashboardShell } from '@/components/dashboard-shell';
import { ErrorState } from '@/components/error-state';
import { LoadingState } from '@/components/loading-state';
import { MetricCard } from '@/components/metric-card';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useDashboardQuery } from '@/hooks/use-dashboard-query';
import { OverviewView } from '@/lib/types';

export default function HomePage() {
  const { data, loading, error } = useDashboardQuery<OverviewView>('/api/v1/overview');
  return (
    <DashboardShell>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-semibold tracking-tight">总览</h2>
          <p className="mt-2 text-sm text-muted">看当前阶段、当前基线、最近实验与当前卡点。</p>
        </div>
        {loading ? <LoadingState /> : null}
        {error ? <ErrorState message={error} /> : null}
        {data ? (
          <>
            <div className="grid gap-4 md:grid-cols-3 xl:grid-cols-6">
              <MetricCard title="当前阶段" value={data.current_phase} />
              <MetricCard title="当前主线" value={data.current_focus} />
              <MetricCard title="当前临时基线" value={data.current_baseline.variant_name} note={data.current_baseline.experiment_id} />
              <MetricCard title="近 7 天实验数" value={data.recent_experiment_count_7d} />
              <MetricCard title="待复审事项" value={data.pending_reviews_count} />
              <MetricCard title="当前卡点数" value={data.blocked_items_count} />
            </div>
            <div className="grid gap-6 lg:grid-cols-2">
              <Card>
                <CardHeader><CardTitle>最新动态</CardTitle></CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {data.latest_items.map((item) => (
                      <div key={item.id} className="flex items-center justify-between rounded-xl border border-border p-3">
                        <div>
                          <div className="font-medium">{item.title}</div>
                          <div className="text-sm text-muted">{item.id}</div>
                        </div>
                        <Badge tone={item.status}>{item.status}</Badge>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
              <Card>
                <CardHeader><CardTitle>当前待办与近期判断</CardTitle></CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div>
                      <h4 className="mb-2 text-sm font-semibold text-muted">当前未完成事项</h4>
                      <ul className="space-y-2 text-sm">
                        {data.current_draft_focus.map((item) => <li key={item}>- {item}</li>)}
                      </ul>
                    </div>
                    <div>
                      <h4 className="mb-2 text-sm font-semibold text-muted">近期经验判断</h4>
                      <ul className="space-y-2 text-sm">
                        {data.recent_test_judgements.map((item) => <li key={item}>- {item}</li>)}
                      </ul>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
            <div className="rounded-2xl border border-border bg-card p-5 shadow-panel">
              <div className="mb-3 flex items-center justify-between">
                <h3 className="text-base font-semibold">快速跳转</h3>
                <Link href="/experiments">查看全部实验</Link>
              </div>
              <div className="text-sm text-muted">当前基线：{data.current_baseline.title}</div>
            </div>
          </>
        ) : null}
      </div>
    </DashboardShell>
  );
}
