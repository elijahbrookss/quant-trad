import { DEFAULT_ATM_TEMPLATE, cloneATMTemplate } from '../../components/atm/ATMConfigForm.jsx'

const STRATEGY_FORM_DEFAULT = {
  name: '',
  description: '',
  timeframe: '15m',
  provider_id: '',
  venue_id: '',
  instrument_slots: [],
  atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
}

const RULE_FORM_DEFAULT = {
  name: '',
  description: '',
  action: 'buy',
  trigger: {
    indicator_id: '',
    output_name: '',
    event_key: '',
  },
  guards: [],
  enabled: true,
}

const INSTRUMENT_FORM_DEFAULT = {
  symbol: '',
  provider_id: '',
  venue_id: '',
  datasource: '',
  exchange: '',
  tick_size: '',
  tick_value: '',
  contract_size: '',
  min_order_size: '',
  base_currency: '',
  quote_currency: '',
  maker_fee_rate: '',
  taker_fee_rate: '',
  can_short: false,
  short_requires_borrow: false,
  has_funding: false,
  expiry_ts: '',
  instrument_type: '',
}

const MIN_RISK_MULTIPLIER = 0.01
const MIN_BASE_RISK = 1

const RISK_DEFAULTS = Object.freeze({
  atrPeriod: 14,
  atrMultiplier: 1,
  baseRiskPerTrade: '',
  globalRiskMultiplier: 1,
})

export {
  STRATEGY_FORM_DEFAULT,
  RULE_FORM_DEFAULT,
  INSTRUMENT_FORM_DEFAULT,
  MIN_RISK_MULTIPLIER,
  MIN_BASE_RISK,
  RISK_DEFAULTS,
}
