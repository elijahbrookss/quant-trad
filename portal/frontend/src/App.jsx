import { ChartComponent } from './components/ChartComponent/ChartComponent'

export default function App() {
  return (
    <div className="bg-neutral-900 text-white min-h-screen p-5">
      <h1 className="text-3xl font-bold text-center mt-10">
        QuantTrad Lab
      </h1>

      <div className="max-w-7xl mx-auto mt-10 p-5 bg-neutral-800 rounded-lg shadow-lg">
        <ChartComponent />
      </div>
    </div>
  )
}