import { CandlestickSeries, createChart, CrosshairMode, LineStyle } from 'lightweight-charts';
import React, { useEffect, useRef } from 'react';

function generateMockOHLC(
  count = 1000,
  basePrice = 100,
  volatility = 2,
  intervalSec = 60 * 60
) {
  const now = Math.floor(Date.now() / 1000);
  const data = [];
  let lastClose = basePrice;

  for (let i = 0; i < count; i++) {
    const time = now - (count - i) * intervalSec;
    const open = lastClose;
    const change = (Math.random() * 2 - 1) * volatility;
    const close = open + change;
    const high =
      Math.max(open, close) + Math.random() * (volatility * 0.5);
    const low =
      Math.min(open, close) - Math.random() * (volatility * 0.5);

    data.push({ time, open, high, low, close });
    lastClose = close;
  }

  return data;
}


export const ChartComponent = () => {
  const chartContainerRef = useRef();

  useEffect(() => {
    
    const chartOptions = {
      layout: {
        textColor: '#DDD',
        background: { color: '#1E1E1E' },
      },
      grid: {
        vertLines: {
          color: "#444",
        },
        horzLines: {
          color: "#444",
        }
      }
    };

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };

  const chart = createChart(chartContainerRef.current, chartOptions);

   chart.timeScale().fitContent();

    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      wickUpColor: 'rgb(54, 116, 217)',
      upColor: 'rgb(54, 116, 217)',
      wickDownColor: 'rgb(225, 50, 85)',
      downColor: 'rgb(225, 50, 85)',
      borderVisible: false,
    });
    chart.applyOptions({
        crosshair: {
            mode: CrosshairMode.Normal,

            vertLine: {
                width: 8,
                color: '#C3BCDB45',
                style: LineStyle.Solid,
                labelBackgroundColor: '#black',
            },

            horzLine: {
                color: '#C3BCDB70',
                labelBackgroundColor: '#black',
            },
        },
    });

    candlestickSeries.setData(generateMockOHLC());

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, []);

  return <div ref={chartContainerRef} className='h-full w-full bg-transparent' />;
}