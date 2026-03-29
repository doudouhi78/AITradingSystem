'use client';

import Link from 'next/link';

import { ScatterChart } from '@/components/charts/scatter-chart';
import { DashboardShell } from '@/components/dashboard-shell';
import { ErrorState } from '@/components/error-state';
import { LoadingState } from '@/components/loading-state';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableCell, TableHead } from '@/components/ui/table';
import { useDashboardQuery } from '@/hooks/use-dashboard-query';
import { ExperimentListView } from '@/lib/types';

export default function ExperimentsPage() {
  const { data, loading, error } = useDashboardQuery<ExperimentListView>('/api/v1/experiments?limit=50');
  return (
    <DashboardShell>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-semibold tracking-tight">实验中心</h2>
          <p className="mt-2 text-sm text-muted">看实验列表、基线与变体关系、关键指标分布。</p>
        </div>
        {loading ? <LoadingState /> : null}
        {error ? <ErrorState message={error} /> : null}
        {data ? (
          <>
            <Card>
              <CardHeader><CardTitle>收益 vs 回撤</CardTitle></CardHeader>
              <CardContent><ScatterChart items={data.items} /></CardContent>
            </Card>
            <Card>
              <CardHeader><CardTitle>实验列表</CardTitle></CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <thead>
                      <tr>
                        <TableHead>实验</TableHead>
                        <TableHead>状态</TableHead>
                        <TableHead>基线来源</TableHead>
                        <TableHead>年化收益</TableHead>
                        <TableHead>最大回撤</TableHead>
                        <TableHead>Sharpe</TableHead>
                        <TableHead>交易数</TableHead>
                      </tr>
                    </thead>
                    <tbody>
                      {data.items.map((item) => (
                        <tr key={item.experiment_id}>
                          <TableCell>
                            <div className="font-medium">{item.variant_label}</div>
                            <Link href={`/experiments/${item.experiment_id}`} className="text-xs text-muted">{item.experiment_id}</Link>
                          </TableCell>
                          <TableCell><Badge tone={item.status}>{item.status}</Badge></TableCell>
                          <TableCell>{item.baseline_of || '-'}</TableCell>
                          <TableCell>{item.annualized_return.toFixed(4)}</TableCell>
                          <TableCell>{item.max_drawdown.toFixed(4)}</TableCell>
                          <TableCell>{item.sharpe.toFixed(4)}</TableCell>
                          <TableCell>{item.trade_count}</TableCell>
                        </tr>
                      ))}
                    </tbody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </>
        ) : null}
      </div>
    </DashboardShell>
  );
}
