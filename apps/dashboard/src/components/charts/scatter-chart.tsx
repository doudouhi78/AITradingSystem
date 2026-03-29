'use client';

import ReactECharts from 'echarts-for-react';

import { ExperimentListItemView } from '@/lib/types';

export function ScatterChart({ items }: { items: ExperimentListItemView[] }) {
  const option = {
    tooltip: {
      trigger: 'item',
      formatter: (params: { data: [number, number, string] }) => `${params.data[2]}<br/>收益: ${params.data[0]}<br/>回撤: ${params.data[1]}`,
    },
    xAxis: { name: '年化收益', type: 'value' },
    yAxis: { name: '最大回撤', type: 'value' },
    series: [
      {
        type: 'scatter',
        symbolSize: 14,
        data: items.map((item) => [item.annualized_return, item.max_drawdown, item.variant_label]),
      },
    ],
  };
  return <ReactECharts option={option} style={{ height: 320 }} />;
}
