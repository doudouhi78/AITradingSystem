export function JsonBlock({ title, value }: { title: string; value: unknown }) {
  return (
    <section className="rounded-2xl border border-border bg-card p-4 shadow-panel">
      <h3 className="mb-3 text-sm font-semibold text-muted">{title}</h3>
      <pre className="overflow-x-auto whitespace-pre-wrap text-sm leading-6">{JSON.stringify(value, null, 2)}</pre>
    </section>
  );
}
