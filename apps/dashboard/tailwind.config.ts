import type { Config } from 'tailwindcss';

export default {
  content: ['./src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        background: '#f6f4ef',
        foreground: '#111827',
        card: '#ffffff',
        border: '#d1d5db',
        muted: '#6b7280',
        accent: '#0f766e',
        danger: '#b91c1c',
        warning: '#b45309',
      },
      boxShadow: {
        panel: '0 8px 32px rgba(17, 24, 39, 0.08)',
      },
    },
  },
  plugins: [],
} satisfies Config;
