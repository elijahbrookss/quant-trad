/* global process */

import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite' 

// https://vite.dev/config/
export function resolveHmrClientPort(value) {
  if (value == null || String(value).trim() === '') return undefined
  const port = Number(value)
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`VITE_HMR_CLIENT_PORT must be a valid TCP port; received ${value}`)
  }
  return port
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiTarget = env.VITE_API_PROXY_TARGET || 'http://localhost:8000'
  const hmrClientPort = resolveHmrClientPort(env.VITE_HMR_CLIENT_PORT)
  return {
    plugins: [tailwindcss(), react()],
    optimizeDeps: {
      include: ['three', '@react-three/fiber', '@react-three/drei'],
    },
    build: {
      rollupOptions: {
        input: {
          main: 'index.html',
          v2: 'v2.html',
        },
        output: {
          manualChunks: {
            react: ['react', 'react-dom', 'react-router-dom'],
            charts: ['lightweight-charts'],
            dates: ['flatpickr', 'react-flatpickr', 'flowbite-datepicker'],
            ui: ['@headlessui/react', 'lucide-react'],
          },
        },
      },
    },
    server: {
      hmr: hmrClientPort
        ? { clientPort: hmrClientPort }
        : undefined,
      proxy: {
        '/api': apiTarget,
      },
    },
    content: [
      './src/**/*.{js,jsx,ts,tsx}',
      './public/index.html',
      './node_modules/flatpickr/**/*.{js,jsx,ts,tsx,html,css,json}',
      './node_modules/react-flatpickr/**/*.{js,jsx,ts,tsx,html,css,json}',
    ],
  }
})
