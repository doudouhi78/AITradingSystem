import * as React from 'react';

import { cn } from '@/lib/utils';

const statusMap: Record<string, string> = {
  completed: 'bg-emerald-50 text-emerald-700',
  approved: 'bg-emerald-50 text-emerald-700',
  recorded: 'bg-slate-100 text-slate-700',
  baseline_candidate: 'bg-cyan-50 text-cyan-700',
  stack_smoke_passed: 'bg-teal-50 text-teal-700',
  record_only: 'bg-amber-50 text-amber-700',
  blocked: 'bg-red-50 text-red-700',
};

export function Badge({ className, tone, children }: { className?: string; tone?: string; children: React.ReactNode }) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full px-2.5 py-1 text-xs font-medium',
        tone ? statusMap[tone] ?? 'bg-slate-100 text-slate-700' : 'bg-slate-100 text-slate-700',
        className,
      )}
    >
      {children}
    </span>
  );
}
