import Link from 'next/link';
import { ReactNode } from 'react';

export function DashboardShell({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-background">
      <div className="mx-auto flex max-w-7xl gap-6 px-6 py-8">
        <aside className="w-60 shrink-0 rounded-2xl border border-border bg-card p-5 shadow-panel">
          <h1 className="mb-6 text-lg font-semibold">第二阶段研究驾驶舱</h1>
          <nav className="space-y-2 text-sm">
            <Link className="block rounded-lg px-3 py-2 hover:bg-slate-50" href="/">总览</Link>
            <Link className="block rounded-lg px-3 py-2 hover:bg-slate-50" href="/experiments">实验中心</Link>
            <Link className="block rounded-lg px-3 py-2 hover:bg-slate-50" href="/flow">流转与问题</Link>
          </nav>
        </aside>
        <main className="min-w-0 flex-1">{children}</main>
      </div>
    </div>
  );
}
