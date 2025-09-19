import {CrosshairMode, LineStyle} from 'lightweight-charts'
 
 export const options = {
      layout: {
        textColor: '#DDD',
        background: { color: '#1E1E1E' },
      },
      grid: {
        vertLines: { color: "#444" },
        horzLines: { color: "#444" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          width: 8,
          color: '#C3BCDB45',
          style: LineStyle.Solid,
          labelBackgroundColor: '#000',
        },
        horzLine: {
          color: '#C3BCDB70',
          labelBackgroundColor: '#000',
        },
      }
    }

export const seriesOptions = {
      wickUpColor: 'rgb(54, 116, 217)',
      upColor: 'rgb(54, 116, 217)',
      wickDownColor: 'rgb(225, 50, 85)',
      downColor: 'rgb(225, 50, 85)',
      borderVisible: false,
    }