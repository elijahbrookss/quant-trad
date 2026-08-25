import react from '@vitejs/plugin-react'
import { defineConfig } from 'vitest/config'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    setupFiles: ['./tests/setupVitest.js'],
    include: [
      'src/components/__tests__/DeleteIndicatorModal.test.jsx',
      'src/components/__tests__/IndicatorCard.test.jsx',
    ],
    clearMocks: true,
    restoreMocks: true,
  },
})
