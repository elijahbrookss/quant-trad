export function RoomLoadingFrame({ room = 'workspace', detail = 'Synchronizing durable evidence' }) {
  return (
    <section className="qt2-loading-frame" role="status" aria-live="polite" aria-label={`Loading ${room}`}>
      <div className="qt2-loading-frame-ambient" aria-hidden="true" />
      <div className="qt2-loading-frame-chrome" aria-hidden="true">
        <div className="qt2-loading-frame-header">
          <span>QT / Operator console</span>
          <span className="qt2-loading-frame-signal"><i />Live sync</span>
        </div>
        <div className="qt2-loading-frame-copy">
          <span>Opening workspace</span>
          <strong>{room}</strong>
        </div>
        <div className="qt2-loading-frame-summary">
          {[0, 1, 2].map((item) => (
            <div key={item}>
              <span />
              <strong />
              <i />
            </div>
          ))}
        </div>
        <div className="qt2-loading-frame-panels">
          {[0, 1].map((panel) => (
            <div key={panel}>
              <span />
              <i />
              <i />
              <i />
            </div>
          ))}
        </div>
        <div className="qt2-loading-frame-footer">
          <span>{detail}</span>
          <i><b /><b /><b /></i>
        </div>
      </div>
    </section>
  )
}
