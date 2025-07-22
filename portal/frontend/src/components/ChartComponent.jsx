import { CandlestickSeries, createChart } from 'lightweight-charts';
import React, { useEffect, useRef } from 'react';

function generateMockOHLC(count = 50) {
  // timestamp, open, high, low, close
  const now = Date.now();
  return Array.from({ length: count }).map((_, i) => ({
    time: (now - (count - i) * 60000) / 1000,
    open: Math.random() * 100,
    high: Math.random() * 100 + 5,
    low: Math.random() * 100 - 5,
    close: Math.random() * 100,
  }));
}


export const ChartComponent = () => {
  const chartContainerRef = useRef();

  useEffect(() => {
    
    const chartOptions = {
      layout: {
        textColor: 'black',
        backgroundColor: { type: 'solid', color: 'white' },
      },
    };

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };

    const chart = createChart(chartContainerRef.current, chartOptions);
   chart.timeScale().fitContent();

    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#4FFF00',
      downColor: '#FF4976',
      borderVisible: false,
      wickVisible: true,
    });
    candlestickSeries.setData(generateMockOHLC(50));

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, []);

  return <div ref={chartContainerRef} style={{ width: '1000px', height: '500px' }} />;
}