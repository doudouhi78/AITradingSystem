import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

export function MetricCard({ title, value, note }: { title: string; value: string | number; note?: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-3xl font-semibold tracking-tight">{value}</div>
        {note ? <p className="mt-2 text-sm text-muted">{note}</p> : null}
      </CardContent>
    </Card>
  );
}
