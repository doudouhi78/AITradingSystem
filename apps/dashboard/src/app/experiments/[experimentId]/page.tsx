'use client';

import { useParams } from 'next/navigation';

import { DashboardShell } from '@/components/dashboard-shell';
import { ErrorState } from '@/components/error-state';
import { JsonBlock } from '@/components/json-block';
import { LoadingState } from '@/components/loading-state';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useDashboardQuery } from '@/hooks/use-dashboard-query';
import { ExperimentDetailView } from '@/lib/types';

export default function ExperimentDetailPage() {
  const params = useParams<{ experimentId: string }>();
  const experimentId = params.experimentId;
  const { data, loading, error } = useDashboardQuery<ExperimentDetailView>(`/api/v1/experiments/${experimentId}`);
  return (
    <DashboardShell>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-semibold tracking-tight">研究链详情</h2>
          <p className="mt-2 text-sm text-muted">查看单个实验的任务、规则、数据、验证、风险、复审和工件。</p>
        </div>
        {loading ? <LoadingState /> : null}
        {error ? <ErrorState message={error} /> : null}
        {data ? (
          <>
            <Card>
              <CardHeader><CardTitle>阶段进度</CardTitle></CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-3">
                  {data.stage_progress.map((item) => <Badge key={item.stage_name} tone={item.stage_status}>{item.stage_name}</Badge>)}
                </div>
              </CardContent>
            </Card>
            <div className="grid gap-4 xl:grid-cols-2">
              <JsonBlock title="任务单" value={data.task_summary} />
              <JsonBlock title="规则表达" value={data.rule_summary} />
              <JsonBlock title="数据快照" value={data.data_snapshot_summary} />
              <JsonBlock title="验证摘要" value={data.validation_summary} />
              <JsonBlock title="风险与仓位" value={data.risk_summary} />
              <JsonBlock title="复审" value={data.review_summary} />
              <JsonBlock title="审批状态" value={data.approval_summary} />
              <JsonBlock title="工件链接" value={data.artifact_links} />
            </div>
          </>
        ) : null}
      </div>
    </DashboardShell>
  );
}
