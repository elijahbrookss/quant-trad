const TabPanel = ({ active, children }) => {
  if (!active) return null
  return <div className="p-6">{children}</div>
}

export default TabPanel
