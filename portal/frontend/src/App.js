import logo from './logo.svg';
import './App.css';
import {ChartComponent} from './components/ChartComponent.jsx';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <div className="tradingview-chart-container">
          <ChartComponent />
        </div>
      </header>
    </div>
  );
}

export default App;
