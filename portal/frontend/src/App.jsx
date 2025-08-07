import { ChartStateProvider } from './contexts/ChartStateContext'
import { ChartComponent } from './components/ChartComponent/ChartComponent'
import { TabManager } from './components/TabManager'

export default function App() {
  const chartId = 'main'


  return (
    <ChartStateProvider>
      <div className="bg-neutral-900 text-white min-h-screen p-5">
        <h1 className="text-3xl font-bold text-center mt-10">QuantTrad Lab</h1>

        <div className="max-w-7xl mx-auto mt-10 p-5 bg-neutral-800 rounded-lg shadow-lg">
          <ChartComponent chartId={chartId} />
        </div>

        <div className="max-w-7xl mx-auto mt-10 p-5 bg-neutral-800 rounded-lg shadow-lg">
          <TabManager chartId={chartId} />
        </div>
      </div>
    </ChartStateProvider>
  )
}
