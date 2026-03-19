import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite' 

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiTarget = env.VITE_API_PROXY_TARGET || 'http://localhost:8000'
  return {
    plugins: [tailwindcss(), react()],
    build: {
      rollupOptions: {
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
