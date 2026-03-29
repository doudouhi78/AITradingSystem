export const API_BASE_URL =
  process.env.NEXT_PUBLIC_DASHBOARD_API_BASE_URL ?? 'http://127.0.0.1:8000';

export async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}
