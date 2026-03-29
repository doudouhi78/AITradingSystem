'use client';

import { useEffect, useState } from 'react';

import { fetchJson } from '@/lib/api';

export function useDashboardQuery<T>(path: string) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);
    fetchJson<T>(path)
      .then((payload) => {
        if (!mounted) return;
        setData(payload);
      })
      .catch((err: Error) => {
        if (!mounted) return;
        setError(err.message);
      })
      .finally(() => {
        if (!mounted) return;
        setLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, [path]);

  return { data, loading, error };
}
