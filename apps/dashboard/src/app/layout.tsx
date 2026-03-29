import './globals.css';

import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: '第二阶段研究驾驶舱',
  description: '本地只读驾驶舱',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}
