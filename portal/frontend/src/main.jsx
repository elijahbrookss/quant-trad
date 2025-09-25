import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import { AccentColorProvider } from './contexts/AccentColorContext.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <AccentColorProvider>
      <App />
    </AccentColorProvider>
  </StrictMode>,
)
