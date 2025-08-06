import { createContext } from 'react'

export const ChartContext = createContext({
  symbol:   '',
  interval: '',
  start:    '',
  end:      '',
})
