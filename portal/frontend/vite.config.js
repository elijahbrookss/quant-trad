import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite' 

// https://vite.dev/config/
export default defineConfig({
  plugins: [tailwindcss(), react()],
  content: [
    './src/**/*.{js,jsx,ts,tsx}',
    './public/index.html',
    './node_modules/flatpickr/**/*.{js,jsx,ts,tsx,html,css,json}',
    './node_modules/react-flatpickr/**/*.{js,jsx,ts,tsx,html,css,json}',
  ],
})
