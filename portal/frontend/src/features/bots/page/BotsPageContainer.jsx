import { BotsPageView } from './BotsPageView.jsx'
import { useBotsPageController } from './useBotsPageController.js'

export function BotsPageContainer() {
  const controller = useBotsPageController()
  return <BotsPageView {...controller} />
}
