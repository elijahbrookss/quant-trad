import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { ResearchText, ResearchScope, instrumentEntries } from './ResearchText.jsx'
describe('readable research text', () => {
 it('renders safe semantic paragraphs, headings, bullets and emphasis without executing HTML', () => {
  const { container } = render(<ResearchText text={'Meaning first.\n\n## Support\n- **441 pairs** retained\n- Limited coverage\n\n<script>alert(1)</script> [not a link](javascript:alert(1))'} />)
  expect(screen.getByRole('heading', { name: 'Support' })).toBeTruthy()
  expect(screen.getAllByRole('listitem')).toHaveLength(2)
  expect(container.querySelector('strong').textContent).toBe('441 pairs')
  expect(container.querySelector('script')).toBeNull();expect(container.querySelector('a')).toBeNull()
 })
 it('preserves late limitations rather than truncating a historical report', () => {
  render(<ResearchText text={'Evidence.\n\n'.repeat(50)+'LIMIT: no raw OHLCV.'} />)
  expect(screen.getByText('LIMIT: no raw OHLCV.')).toBeTruthy()
 })
 it('shows authoritative multiple instruments and one missing-scope note', () => {
  const { rerender }=render(<ResearchScope item={{kind:'study',id:'s'}} scope={{basis:'selected_summary_checks',complete:true,items:[{instrument_id:'btc',instrument:{symbol:'BTC-USD'}},{instrument_id:'eth',instrument:{symbol:'ETH-USD'}}]}} />)
  expect(screen.getByText('BTC-USD · ETH-USD')).toBeTruthy()
  rerender(<ResearchScope item={{kind:'study',id:'s'}} />)
  expect(screen.getByText(/Scope metadata is not recorded at Study level/)).toBeTruthy()
  expect(screen.queryByText('Unknown')).toBeNull()
 })
})

it('keeps distinct instrument IDs when catalog symbols collide',()=>{
 const entries=instrumentEntries([{instrument_id:'spot',instrument:{symbol:'BTC-USDT',instrument_type:'SPOT',exchange:'A'}},{instrument_id:'perp',instrument:{symbol:'BTC-USDT',instrument_type:'PERPETUAL',exchange:'B'}}])
 expect(entries).toHaveLength(2);expect(entries[0].label).not.toBe(entries[1].label)
})


it('disambiguates equal symbol, type and venue with source and exact ID when needed', () => {
 const row = { symbol: 'BTC-USD', instrument_type: 'SPOT', exchange: 'Coinbase', datasource: 'native' }
 const entries = instrumentEntries([{instrument_id:'first',instrument:row},{instrument_id:'second',instrument:row},{instrument_id:'third',instrument:{...row,datasource:'other'}}])
 expect(new Set(entries.map(entry => entry.label)).size).toBe(3)
 expect(entries.find(entry => entry.key === 'first').label).toContain('[first]')
 expect(entries.find(entry => entry.key === 'third').label).toContain('other')
})
