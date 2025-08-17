import React from 'react';
import ThemeToggle from './ThemeToggle';

const Header = ({ insightCount = 0 }) => {
  return (
    <header className="app-header">
      <div className="header-content">
        <div className="logo-section">
          <h1 className="logo">SignalEdge</h1>
          <p className="tagline">Separate signal from noise.</p>
        </div>
        <div className="header-actions">
          <div className="header-stats">
            <div className="stat">
              <span className="stat-number">{insightCount}</span>
              <span className="stat-label">Insights</span>
            </div>
          </div>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
};

export default Header;
