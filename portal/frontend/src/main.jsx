import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'
import { AccentColorProvider } from './contexts/AccentColorContext.jsx'
import { PortalSettingsProvider } from './contexts/PortalSettingsContext.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <AccentColorProvider>
      <PortalSettingsProvider>
        <App />
      </PortalSettingsProvider>
    </AccentColorProvider>
  </StrictMode>,
)
