export function ErrorState({ message }: { message: string }) {
  return <div className="rounded-2xl border border-danger bg-red-50 p-6 text-sm text-danger shadow-panel">{message}</div>;
}
