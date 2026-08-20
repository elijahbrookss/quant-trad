import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import './v2/v2.css'
import './v2/v2-operator.css'
import { AppV2 } from './v2/AppV2.jsx'
import { AccentColorProvider } from './contexts/AccentColorContext.jsx'
import { PortalSettingsProvider } from './contexts/PortalSettingsContext.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <AccentColorProvider>
      <PortalSettingsProvider>
        <AppV2 />
      </PortalSettingsProvider>
    </AccentColorProvider>
  </StrictMode>,
)
